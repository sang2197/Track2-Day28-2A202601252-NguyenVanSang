# ANSWERS - Lab 28 Track 2 (làm cá nhân)

Họ và tên: Nguyễn Văn Sáng
MSSV: 2A202601252
Ngày nộp: 03/09/2026

## 1. Đã làm được gì

- Viết xong 4 hàm còn thiếu trong `src/lab28_platform/integration_tasks.py`:
  - `event_headers`: gắn mã theo dõi (traceparent) và khóa chống trùng (idempotency-key) vào header Kafka.
  - `dedupe_latest`: khi Kafka gửi lại dữ liệu cũ, giữ lại đúng một bản mới nhất cho mỗi khóa.
  - `feast_online_request`: tạo đúng yêu cầu gọi Feast lấy đặc trưng của người hỏi.
  - `readiness_status`: phân biệt `ready`, `degraded`, `not_ready` dựa trên lỗi bắt buộc và lỗi không bắt buộc.
- Tất cả kiểm thử đều đạt: `pytest starter-tests tests -q` (87 test pass), `ruff check .` không lỗi.
- Ba script kiểm tra `verify_matrix.py`, `check_portability.py`, `validate_manifests.py` đều trả mã 0 (đạt).
- Đã chạy hệ thống cơ bản bằng Docker: Kafka, API, Gateway (Envoy), Feast, Qdrant, MLflow, Prometheus, Grafana, Jaeger.
- Đã tạo topic Kafka, index 13 tài liệu vào Qdrant, phát hành phiên bản mô hình v1 lên MLflow (champion), seed 13 document + 12 feedback qua gateway không có bản ghi bị từ chối.
- Đã thử promotion/rollback MLflow: phát hành thêm v2 thành champion, sau đó rollback về v1 thành công (không sửa mã).
- Kiểm tra `lab28 ready` trả về `degraded` (đúng như kỳ vọng khi chưa nối vLLM thật).
- Thu được evidence cho IP01 (Kafka header thật), IP04 (gọi Feast lấy đặc trưng, dữ liệu mặc định vì chưa có Delta), IP05 (Qdrant), IP06 (MLflow + rollback), IP07 (xác nhận chưa phải vLLM thật), IP08 (gateway rate limit 10 req/s có 429), IP09 (Prometheus target đều up, Grafana có dashboard).
- Đã đo thử load test (200 request, 8 worker) lên `/ready` qua gateway để có số liệu P50/P95/P99.
- Đã lưu kết quả `scripts/validate_manifests.py` (K8s/GitOps) làm evidence.

## 1.1 Phân tích load test (bottleneck)

Chạy `load-tests/run_profile.py --requests 200 --workers 8` nhắm vào `/ready` qua gateway (`evidence/load-profile.json`):

- P50 ~3.7 ms, P95 ~444 ms, P99 ~548 ms.
- Chỉ 21/200 request trả về 200, phần lớn còn lại bị chặn (rate limit trả 429, nhưng script gốc gộp mọi lỗi thành mã "0" nên không tách riêng được).

Nguyên nhân nghẽn (bottleneck): gateway Envoy giới hạn 10 request/giây (cấu hình ở `gateway/envoy.yaml`). Khi gửi 200 request gần như cùng lúc bằng 8 luồng, phần lớn vượt quá ngưỡng này nên bị từ chối ngay tại gateway. Đây là hành vi đúng thiết kế của IP08 (bảo vệ hệ thống phía sau khỏi bị quá tải), không phải lỗi của API hay readiness endpoint. Nếu muốn tăng thông lượng thật, cần tăng `max_tokens`/`tokens_per_fill` trong cấu hình rate limit hoặc scale nhiều instance API phía sau, tùy vào SLA mong muốn.

## 2. Phần chưa làm và lý do

Bài có 2 đường chạy nâng cao mà mình chọn không làm, vì mục tiêu chỉ cần đạt điểm cơ bản:

- **Toàn bộ hệ thống (Airflow, Spark, Delta Lake)**: cần chạy `docker compose --profile full`, tốn nhiều tài nguyên và thời gian hơn. Nếu không có bước này thì:
  - IP02 (Kafka đến Airflow) và IP03 (Airflow/Spark đến Delta) không thể chứng minh được.
  - Không có bảng Delta nên IP04 (Feast) chỉ chạy được ở mức kết nối, chưa có dữ liệu thật đã materialize.
- **Nối vLLM thật (GPU)**: cần máy có GPU hoặc dùng Kaggle T4 theo `KAGGLE_GPU_EXTENSION.md`. Mình chưa làm bước này, nên:
  - IP07 báo `not_ready` (không tìm thấy máy chủ vLLM thật) - đây là trạng thái đúng, không phải lỗi.
  - Lệnh `lab28 ask` bị timeout vì API cố gọi vLLM mà không có.
  - IP10 (trace end-to-end) chưa đầy đủ vì thiếu chặng gọi mô hình.

Vì thiếu IP02, IP03, IP07, theo rubric thì điểm tối đa có thể đạt là khoảng 60/100 (mục "10 integration points" và "Correctness & recovery" sẽ bị trừ). Đây là lựa chọn có chủ đích để phù hợp với thời gian và máy cá nhân, không phải do làm sai.

## 3. Một số đánh đổi kỹ thuật (trade-off)

- Chọn `dedupe_latest` so sánh theo cặp `(occurred_at, event_id)` thay vì chỉ theo thời gian, để tránh trường hợp 2 bản tin đến cùng lúc (trùng giây) mà kết quả không ổn định giữa các lần chạy.
- Cổng Gateway trên máy bị trùng (8080 đã có ứng dụng khác dùng), nên mình tạo file `ports.local.template` copy từ `ports.template` và chỉ đổi cổng Gateway sang 8083, không sửa file gốc.
- Khi seed dữ liệu qua gateway, 25 request gửi liên tiếp bị chặn bớt bởi rate limit 10 request/giây của Envoy (đây là tính năng đúng, không phải lỗi). Mình gửi lại có giãn cách thời gian giữa các request để seed sạch dữ liệu ban đầu, đồng thời vẫn giữ cấu hình rate limit gốc để chứng minh IP08 hoạt động đúng ở phần evidence riêng.

## 4. Nếu đưa lên môi trường thật (production) thì còn thiếu gì

- Cần thật sự chạy được Airflow + Spark + Delta để có luồng dữ liệu đầy đủ, không chỉ dừng ở mức "kết nối được".
- Cần một máy chủ vLLM thật (có GPU) để trả lời câu hỏi, hiện tại chỉ mới xác nhận framework sẵn sàng gọi tới, chưa có mô hình trả lời thật.
- Cần thêm giám sát cảnh báo (alert) gửi qua kênh thật (Slack/Email) thay vì chỉ có rule trong Prometheus.
- Cần kiểm tra bảo mật kỹ hơn: hiện tại các cổng đều mở ở localhost cho mục đích học, môi trường thật cần xác thực (auth), TLS, và quản lý secret đúng cách (Vault, K8s secret...).
- Chưa demo được kịch bản sự cố (một thành phần chết rồi phục hồi) do cần chạy toàn bộ hệ thống.

## 5. Đóng góp

Làm cá nhân, tự thực hiện toàn bộ các phần: viết code, chạy Docker, kiểm thử, thu evidence.
