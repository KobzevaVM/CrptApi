package org.example;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static java.net.http.HttpClient.newHttpClient;

public class CrptApi {

    private final static int PERIOD = 20;
    private final static int THREAD_POOL_SIZE = 5;
    private final static int MAX_COUNT_POST_REQUEST = 3;
    private final static String URL = "https://ismp.crpt.ru/api/v3/lk/documents/create";
    private final AtomicInteger requestsCount = new AtomicInteger(0);
    private final int requestLimit;
    private final HttpClient httpClient;

    private final ObjectMapper mapper;
    private ScheduledExecutorService scheduler;

    public CrptApi(TimeUnit timeUnit, int requestLimit) {

        if(requestLimit > 0) {
            this.requestLimit = requestLimit;
            httpClient = newHttpClient();
            mapper = new ObjectMapper();
            scheduler = Executors.newScheduledThreadPool(THREAD_POOL_SIZE);
            scheduler.scheduleAtFixedRate(this::start, 0, PERIOD, timeUnit);
        } else {
            throw new IllegalArgumentException("requestLimit must be positive");
        }

    }

    public void start() {
        requestsCount.set(0);
        System.out.println("Start of scheduler");
    }

    /**
     * Запуск процесса добавление документа
     */
    public synchronized void addDocument(Document document, String signature) {
        executeTask();
        String jsonDocument = convertJson(document);
        executeRequest(jsonDocument, signature);
    }

    /**
     * Доступ к дальнейшему выполнению задания,
     * если в расписании уже выполнено больше заданного числа запросов, потоки ждут
     */
    private void executeTask() {
        while(requestsCount.get() >= requestLimit) {}
        requestsCount.incrementAndGet();
    }
    /**
     * Конвертация в json
     */
    private String convertJson(Document document) {
        try {
            return mapper.writeValueAsString(document);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Post запрос на сервер
     */
    public void executeRequest(String jsonDocument, String signature) {

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(URL))
                .header("Content-Type", "application/json")
                .header("Signature", signature)
                .POST(HttpRequest.BodyPublishers.ofString(jsonDocument))
                .build();

        HttpResponse<String> response;
        try {
            response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
        System.out.println("Post document " + signature);
    }
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Document {
        @JsonProperty("description")
        private Description description;
        @JsonProperty("doc_id")
        private String docId;
        @JsonProperty("doc_status")
        private String docStatus;
        @JsonProperty("doc_type")
        private DocType docType;
        @JsonProperty("import_request")
        private boolean importRequest;
        @JsonProperty("owner_inn")
        private String ownerInn;
        @JsonProperty("participant_inn")
        private String participantInn;
        @JsonProperty("producer_inn")
        private String producerInn;
        @JsonProperty("production_date")
        private String productionDate;
        @JsonProperty("production_type")
        private String productionType;
        @JsonProperty("products")
        private List<Product> products;
        @JsonProperty("reg_date")
        private String regDate;
        @JsonProperty("reg_number")
        private String regNumber;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Product {
        @JsonProperty("certificate_document")
        private String certificateDocument;
        @JsonProperty("certificate_document_date")
        private String certificateDocumentDate;
        @JsonProperty("certificate_document_number")
        private String certificateDocumentNumber;
        @JsonProperty("owner_inn")
        private String ownerInn;
        @JsonProperty("producer_inn")
        private String producerInn;
        @JsonProperty("production_date")
        private String productionDate;
        @JsonProperty("tnved_code")
        private String tnvedCode;
        @JsonProperty("uit_code")
        private String uitCode;
        @JsonProperty("uitu_code")
        private String uituCode;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Description {
        @JsonProperty("participant_inn")
        private String participantInn;
    }
    public enum DocType {
        LP_INTRODUCE_GOODS,
        LP_CONTRACT_COMISSIONING;
    }

    public static void main(String[] args) throws InterruptedException {
        CrptApi crptApi = new CrptApi(TimeUnit.SECONDS, MAX_COUNT_POST_REQUEST);

        //TODO for testing. Можно создать несколько потоков и вызвать метод start().
        // Потоки будут выполнять по 3 шт. в 20 секунд непоследовательно
        Runnable task = () -> {
            crptApi.addDocument(crptApi.createDocument(), "Signature" + Thread.currentThread().getName());
        };

        Thread thread1 = new Thread(task);
        thread1.start();

        //Прерывание расписания не настроено, т.к. возможно программа должна работать в фоновом режиме всегда и ждать файлы
    }

    private Document createDocument() {
        List<Product> products = new ArrayList<>();
        for(int i = 0; i < 5; i++) {
            Product product = new Product("certDoc" + i, "2024-06-04",
                    "certDocNum" + i,"ownerInn", "prodInn",
                    "2024-06-04", "tnvedCode" + i, "uitCode" + i,
                    "uituCode" + i);
            products.add(product);
        }
        Description description = new Description("partInn");

        Document document = new Document(description, "docId", "docStatus",
                DocType.LP_INTRODUCE_GOODS, true, "ownerInn", "partInn",
                "prodInn","2024-06-04", "prodType", products,
                "2024-06-04", "regNum");

        return document;
    }
}
