package org.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.Date;

public class WebLogConsumerCmp {
    private static final String BOOTSTRAP_SERVERS = "192.168.150.115:9192,192.168.150.115:9194,192.168.150.125:9192";
    private static final String GROUP_ID = "web-log-consumer-group-cmp";
    private static final String TOPIC = "hr-test-web-log";
    private static final ObjectMapper mapper = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS); // ISO 형식으로 출력

    public static void main(String[] args) throws SQLException {
        // Kafka Consumer 설정
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(List.of(TOPIC));

        // DB 연결 (최종 저장용)
        Connection conn;
        try {
            String jdbcUrl = "jdbc:mysql://192.168.150.100:3306/rwkcp";
            String dbUser = "rwkcp_app";
            String dbPass = "fpemdnemzpdl123$";
            conn = DriverManager.getConnection(jdbcUrl, dbUser, dbPass);
        } catch (SQLException e) {
            System.err.println("JDBC 연결 실패(conn): " + e.getMessage());
            return;
        }

        // DB 연결(참조용)
        Connection connRef;
        try {
            String jdbcUrl = "jdbc:mysql://192.168.150.110:3306/didim_cb";
            String dbUser = "hydrak";
            String dbPass = "fpemdnemzpdl123$";
            connRef = DriverManager.getConnection(jdbcUrl, dbUser, dbPass);
        } catch (SQLException e) {
            System.err.println("JDBC 연결 실패(connRef): " + e.getMessage());
            return;
        }
        System.out.println("Listening for messages...");

        // 성능 측정
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        long totalSelectTime = 0;
        long totalInsertTime = 0;
        long beforeSelectTime = 0;
        long afterSelectTime = 0;
        long beforeInsertTime = 0;
        long afterInsertTime = 0;
        int count = 0;

        // 메시지 컨슈밍
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<String, String> record : records) {
//                System.out.println("Consumed message: "+record.value());

                try {
                    Map<String, Object> data = mapper.readValue(record.value(), Map.class);

                    // ISO 8601 형식의 timestamp 처리
                    String timestampStr = (String) data.get("timestamp");
                    Instant instant = Instant.parse(timestampStr);  // ISO 8601 형식 파싱
                    Timestamp timestamp = Timestamp.from(instant);  // Timestamp로 변환

                    String cusno = (String) data.get("cusno");
                    String url = (String) data.get("url");
                    String httpMethod = (String) data.get("http_method");
                    Integer responseTime = (Integer) data.get("response_time");
                    String ipAddress = (String) data.get("ip_address");
                    Integer statusCode = (Integer) data.get("status_code");
                    String serviceId = (String) data.get("service_id");
                    String prodCd = (String) data.get("prod_cd");

                    // DB 고객 데이터 조회
                    String customerSql = "SELECT CUS_GRADE, CUS_GENDER, CUS_AGE_RANGE FROM TB_BD_CUSTOMER_INFO WHERE CUSNO = ?";
                    String cusGrade = null;
                    String cusGender = null;
                    String cusAgeRange = null;

                    try (PreparedStatement pstmt = connRef.prepareStatement(customerSql)) {
                        pstmt.setString(1, cusno);

                        beforeSelectTime = System.currentTimeMillis();  // 참조 직전
                        try (ResultSet rs = pstmt.executeQuery()) {
                            afterSelectTime = System.currentTimeMillis();  // 참조 직후
                            if (rs.next()) {
                                cusGrade = rs.getString("CUS_GRADE");
                                cusGender = rs.getString("CUS_GENDER");
                                cusAgeRange = rs.getString("CUS_AGE_RANGE");
                            }
                        }
                    }

                    // VVIP 고객만 감지
                    if ("01".equalsIgnoreCase(cusGrade)) {
//                        System.out.println("VVIP Detected: " + cusno);

                        // DB 저장
                        String insertSql = "INSERT INTO TB_HR_TEST_WEB_LOG_CMP (timestamp, cusno, url, http_method, response_time, ip_address, status_code, service_id, prod_cd) " +
                                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";
//                        System.out.println("PreparedStatement sql: "+insertSql);

                        try (PreparedStatement stmt = conn.prepareStatement(insertSql)) {
                            stmt.setTimestamp(1, timestamp);
                            stmt.setString(2, cusno);
                            stmt.setString(3, url);
                            stmt.setString(4, httpMethod);
                            stmt.setInt(5, responseTime);
                            stmt.setString(6, ipAddress);
                            stmt.setInt(7, statusCode);
                            stmt.setString(8, serviceId);
                            stmt.setString(9, prodCd);

                            beforeInsertTime = System.currentTimeMillis();   // 데이터 저장 시간
                            stmt.executeUpdate();
                            afterInsertTime = System.currentTimeMillis();   // 데이터 저장 시간

                            // 시간 누적
                            totalSelectTime += (afterSelectTime - beforeSelectTime);
                            totalInsertTime += (afterInsertTime - beforeInsertTime);
                            count++;

                            // 건별 로그 출력
//                            System.out.printf("[Log] 도착: %s / 참조: %s / 저장: %s / 참조시간: %dms / 저장시간: %dms%n",
//                                    sdf.format(new Date(arrivalTime)),
//                                    sdf.format(new Date(selectionTime)),
//                                    sdf.format(new Date(insertTime)),
//                                    (selectionTime - arrivalTime),
//                                    (insertTime - arrivalTime));

                            // 1,000건마다 평균 출력
                            if (count % 1000 == 0) {
                                System.out.printf("[통계] 처리 건수: %,d / 평균 참조시간: %.2fms / 평균 저장시간: %.2fms%n",
                                        count,
                                        totalSelectTime / (double) count,
                                        totalInsertTime / (double) count);
                            }

                        } catch (SQLException e) {
                            e.printStackTrace();
                            System.err.println("Failed to save record to DB: " + e.getMessage());
                        }
                    }
                } catch (Exception e) {
                    System.err.println("Error processing Kafka record or Redis data: " + e.getMessage());
                }
            }
        }
    }
}