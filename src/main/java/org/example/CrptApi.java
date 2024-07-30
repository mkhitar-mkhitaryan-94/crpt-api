package org.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Класс CrptApi для взаимодействия с API Честного знака, в частности для создания
 * документов для ввода товаров в оборот.
 * Класс является потокобезопасным и поддерживает ограничение по количеству запросов.
 */
public class CrptApi {

    private static final String API_URL = "https://ismp.crpt.ru/api/v3/lk/documents/create";
    private static final Logger logger = LoggerFactory.getLogger(CrptApi.class);

    private final TimeUnit timeUnit;
    private final int requestLimit;
    private final ReentrantLock lock = new ReentrantLock();
    private final AtomicInteger requestCount = new AtomicInteger(0);
    private Instant windowStart = Instant.now();
    private final OkHttpClient httpClient;
    private final ObjectMapper objectMapper;
    private final ScheduledExecutorService scheduler;

    /**
     * Конструктор класса CrptApi с заданными единицами времени и лимитом запросов.
     *
     * @param timeUnit     единицы времени для окна лимита запросов
     * @param requestLimit максимальное количество запросов, разрешенное в указанный интервал времени
     */
    public CrptApi(TimeUnit timeUnit, int requestLimit) {
        this.timeUnit = timeUnit;
        this.requestLimit = requestLimit;
        this.httpClient = new OkHttpClient();
        this.objectMapper = new ObjectMapper();
        this.scheduler = Executors.newSingleThreadScheduledExecutor();
        resetRequestCountPeriodically();
    }

    /**
     * Создает документ в системе CRPT.
     *
     * @param document  объект документа для отправки
     * @param signature цифровая подпись для документа
     * @throws IOException          если произошла ошибка ввода-вывода
     * @throws InterruptedException если текущий поток прерван во время ожидания доступного слота запроса
     */
    public void createDocument(Object document, String signature) throws IOException, InterruptedException {
        logger.info("Создание документа: {}", document);
        waitForAvailableRequestSlot();
        String jsonDocument = objectMapper.writeValueAsString(document);
        RequestBody requestBody = buildRequestBody(jsonDocument, signature);
        Request request = new Request.Builder()
                .url(API_URL)
                .post(requestBody)
                .build();
        executeRequest(request);
    }

    /**
     * Ожидает доступного слота для выполнения запроса, учитывая лимит запросов и интервал времени.
     * Этот метод блокирует текущий поток до тех пор, пока не станет доступен слот для нового запроса.
     *
     * @throws InterruptedException если текущий поток прерван во время ожидания
     */
    private void waitForAvailableRequestSlot() throws InterruptedException {
        lock.lock();
        try {
            resetIfWindowExpired();
            if (isRequestSlotAvailable()) {
                requestCount.incrementAndGet();
                logger.debug("Запрос выполнен, текущий счетчик запросов: {}", requestCount.get());
            } else {
                waitForSlotAvailability();
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Создает тело запроса для отправки документа и подписи.
     *
     * @param jsonDocument строка с сериализованным документом в формате JSON
     * @param signature    цифровая подпись
     * @return объект RequestBody для отправки запроса
     */
    private RequestBody buildRequestBody(String jsonDocument, String signature) {
        ObjectNode rootNode = objectMapper.createObjectNode();
        rootNode.put("document", jsonDocument);
        rootNode.put("signature", signature);
        return RequestBody.create(rootNode.toString(), MediaType.parse("application/json"));
    }

    /**
     * Проверяет, доступен ли слот для выполнения нового запроса.
     *
     * @return true, если слот для нового запроса доступен; false в противном случае
     */
    private boolean isRequestSlotAvailable() {
        return requestCount.get() < requestLimit;
    }

    /**
     * Сбрасывает счетчик запросов, если текущее время превышает время окончания текущего окна.
     * Обновляет начало нового окна и сбрасывает счетчик запросов.
     */
    private void resetIfWindowExpired() {
        Instant now = Instant.now();
        long elapsed = Duration.between(windowStart, now).toMillis();
        long intervalMillis = timeUnit.toMillis(1);

        if (elapsed > intervalMillis) {
            windowStart = now;
            requestCount.set(0);
            logger.debug("Счетчик запросов сброшен, новое окно: {}", windowStart);
        }
    }

    /**
     * Ожидает, пока слот для выполнения запроса не станет доступным.
     * При превышении лимита запросов текущий поток засыпает на оставшееся время до окончания текущего окна.
     *
     * @throws InterruptedException если текущий поток прерван во время ожидания
     */
    private void waitForSlotAvailability() throws InterruptedException {
        long intervalMillis = timeUnit.toMillis(1);
        long sleepTime = intervalMillis - Duration.between(windowStart, Instant.now()).toMillis();

        logger.debug("Достигнут лимит запросов, ожидание: {} миллисекунд", sleepTime);
        Thread.sleep(sleepTime);
        resetIfWindowExpired();
    }


    /**
     * Выполняет HTTP запрос и обрабатывает ответ.
     *
     * @param request объект Request для выполнения
     * @throws IOException если ответ имеет ошибочный статус
     */
    private void executeRequest(Request request) throws IOException {
        logger.info("Отправка запроса: {}", request);
        try (Response response = httpClient.newCall(request).execute()) {
            if (!response.isSuccessful()) {
                String errorMsg = "Неожиданный код ответа: " + response.code();
                logger.error(errorMsg);
                throw new IOException(errorMsg);
            }
            ResponseBody responseBody = response.body();
            if (responseBody == null) {
                String errorMsg = "Ответ не содержит тела";
                logger.error(errorMsg);
                throw new IOException(errorMsg);
            }
            String responseBodyString = responseBody.string();
            logger.info("Ответ: {}", responseBodyString);
        } catch (IOException e) {
            logger.error("Ошибка выполнения запроса", e);
            throw e;
        }
    }

    /**
     * Периодически сбрасывает счетчик запросов, чтобы начать новый интервал времени.
     */
    private void resetRequestCountPeriodically() {
        long period = timeUnit.toMillis(1);
        scheduler.scheduleAtFixedRate(() -> {
            lock.lock();
            try {
                windowStart = Instant.now();
                requestCount.set(0);
                logger.debug("Счетчик запросов сброшен периодически, новое окно: {}", windowStart);
            } finally {
                lock.unlock();
            }
        }, period, period, TimeUnit.MILLISECONDS);
    }

    public static void main(String[] args) {
        CrptApi api = new CrptApi(TimeUnit.SECONDS, 5);
        ObjectNode document = new ObjectMapper().createObjectNode();
        document.put("description", "Sample Description");
        document.put("doc_id", "sampleDocId");
        document.put("doc_status", "NEW");
        document.put("doc_type", "LP_INTRODUCE_GOODS");
        document.put("importRequest", true);
        document.put("owner_inn", "1234567890");
        document.put("participant_inn", "0987654321");
        document.put("producer_inn", "1122334455");
        document.put("production_date", "2023-07-25");
        document.put("production_type", "MANUFACTURED");
        document.putArray("products").addObject()
                .put("certificate_document", "Certificate")
                .put("certificate_document_date", "2023-07-25")
                .put("certificate_document_number", "12345")
                .put("owner_inn", "1234567890")
                .put("producer_inn", "1122334455")
                .put("production_date", "2023-07-25")
                .put("tnved_code", "1234")
                .put("uit_code", "987654321")
                .put("uitu_code", "54321");
        document.put("reg_date", "2023-07-25");
        document.put("reg_number", "Reg12345");

        try {
            api.createDocument(document, "SampleSignature");
        } catch (IOException | InterruptedException e) {
            logger.error("Ошибка создания документа", e);
        }
    }
}
