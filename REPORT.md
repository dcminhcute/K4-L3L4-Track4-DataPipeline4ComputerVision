# REPORT — Lab Day 9

> Điền **bằng lời và số liệu của chính bạn** (lấy từ đầu ra của `scripts/lab.sh step ...` và từ `report/evidence/*.json`). Bài làm công khai trên GitHub; giảng viên đối chiếu với lịch sử commit và với bài của các bạn khác. Mỗi câu 3 đến 8 dòng là đủ.

| | |
|---|---|
| Họ và tên | Doan Quang Minh |
| MSSV (đúng bằng `LAB_STUDENT_ID` trong `.env`) | 2A202602711 |
| GitHub | dcminhcute |
| Trip dùng cho bài tập sập và chạy lại (xem GUIDE, bước 5) | trip-05297 |

## Kết quả cuối

- `dataset_version` của bạn: `072ce76c605a`  (phải khớp `report/evidence/run_summary.json`)
- Số khung hình train / val / test của bạn: train=714, val=131, test=174 (tổng 1019)
- `report/verify.txt`: tạo bằng `python3 scripts/check_submission.py --save-verify` (phải có `27/27 checks passed`).
- Ảnh chụp trong `report/screenshots/`: `airflow_graph.png`, `minio_lake.png`, `metabase_dashboard.png`, và ảnh bài tập sập/chạy lại (log task đỏ thấy trip của bạn).

## Bước 1 · Bronze

### Câu 1
Liệt kê **ít nhất 4 khác biệt** giữa telemetry nguồn A và nguồn B mà bạn thấy ở bronze:

| Khác biệt | Nguồn A | Nguồn B |
|---|---|---|
| Tên cột timestamp | `ts_utc` | `timestamp_ms` |
| Đơn vị tốc độ | `speed_mps` (m/s) | `spd_kmh` (km/h) |
| Timestamp | ISO UTC (`2025-10-07T08:00:00Z`) | Epoch milliseconds (`1759933801509`) |
| Tên cột GPS | `lat`, `lon` | `latitude`, `longitude` |
| Tên cột gia tốc | `acc_x`, `acc_y` | `ax`, `ay` |
| Đơn vị bàn đạp | `gas`, `brake` (tỷ lệ 0-1) | `throttle_pct`, `brake_pct` (0-100) |

Pipeline giữ **mỗi nguồn một bảng bronze riêng** vì hai lý do:
1. **Schema drift**: hai nguồn có cấu trúc hoàn toàn khác nhau (tên cột, đơn vị, định dạng thời gian)
2. **Traceability**: giữ nguyên dữ liệu thô để có thể truy vết nguồn gốc

Giữ **mọi cột ở dạng chữ** (string) vì:
- **Bronze = ghi nguyên bản**: không biến đổi dữ liệu, chỉ thêm
- **Trì hoãn ép kiểu**: để schema được validate kỹ lưỡng ở lớp Silver trước khi chuyển đổi

## Bước 2 · Silver telemetry

### Câu 2
Pipeline đưa hai nguồn về một schema như thế nào:

1. **Đơn vị**: B được chuyển từ km/h sang m/s (chia cho 3.6)
2. **Timestamp**: B được chuyển từ epoch ms sang ISO UTC và **trừ độ lệch đồng hồ 1500ms** (telemetry B lệch 1.5 giây so với video)
3. **Tên cột**: thống nhất thành `speed_mps`, `ts`, `lat`, `lon`

**Bằng chứng**: Thời điểm bắt đầu của B (`2025-10-08 14:30:00.009`) khớp với thời điểm bắt đầu video.

**Bảng quarantine**:
| Drive | Lý do | Số dòng |
|-------|-------|----------|
| drive_a | duplicate | 20 |
| drive_a | speed_missing | 30 |
| drive_a | speed_out_of_range | 3 |
| drive_b | duplicate | 20 |
| drive_b | speed_missing | 30 |
| drive_b | speed_out_of_range | 3 |

Dòng lỗi vào `rejects` thay vì bị xoá vì:
- **Audit trail**: lưu lại để debug và phân tích nguyên nhân lỗi
- **Compliance**: không xoá dữ liệu, chỉ cách ly
- **Tái sử dụng**: có thể khôi phục nếu quy tắc thay đổi

### Câu 3
**Test `max_gap_ms` là `warn`**: vì mất tín hiệu cảm biến là sự cố tạm thời, không phải lỗi dữ liệu. Pipeline tiếp tục chạy nhưng cảnh báo để theo dõi.

**Test `in_range` là `error`**: vì tốc độ vô lý (>70 m/s ≈ 252 km/h) là lỗi nghiêm trọng, không thể chấp nhận trong dữ liệu huấn luyện.

**Kết quả thí nghiệm `break-gate`**:
- Khi nới max_speed_mps từ 70 lên 1000: `in_range_silver_telemetry_speed_mps__70__0` bắt được **6 dòng** có tốc độ vô lý
- Cổng chất lượng đã ngăn 6 dòng "rác" đi vào silver

Điều này cho thấy **cổng chất lượng là lớp bảo vệ cuối cùng**: dù có lỗi ở transformation, test vẫn bắt được dữ liệu xấu trước khi đến gold.

## Bước 3 · Silver frames

### Câu 4
**Dữ liệu idempotency**:
- Trước: 1246 dòng, fingerprint `6c7480dfde7e`
- Sau 2 lần chạy lại: 1246 dòng, fingerprint `6c7480dfde7e` → **KHÔNG ĐỔI**
- Khi dùng `append` (sai): 120 dòng cho 60 khung → **NHÂN ĐÔI 2 lần**

Delta `overwrite` theo partition an toàn hơn vì:
- **Atomic**: partition được thay thế nguyên vẹn, không có trạng thái lẫn lộn
- **Time travel**: có thể quay lại phiên bản trước nếu có lỗi
- **Crash recovery**: nếu chạy lại giữa chừng, dữ liệu cũ không bị ảnh hưởng
- So với xoá rồi ghi lại: nếu ghi mới bị gián đoạn, có thể mất cả cũ lẫn mới

### Câu 5
**Khung đen vs khung tối**:
- **Khung đen** (is_black): luma < 12, camera tắt hoặc che hoàn toàn → bị loại khỏi gold
- **Khung tối** (is_dark): luma thấp nhưng > 12, xe đang trong đường hầm/đỗ bóng râm → **gắn cờ nhưng GIỮ LẠI** trong silver

Pipeline xử lý: đen = loại, tối = giữ lại vì khung tối vẫn có thông tin hữu ích.

**Bảng ngưỡng khung trùng**:
| dHash khác tối đa | số khung trùng, drive A | số khung trùng, drive B |
|---|---|---|
| 0 | 61 | 8 |
| 2 | 149 | 53 |
| 6 | 363 | 244 |

**Ngưỡng 2 (đang dùng)**: cho phép 2 bit khác nhau giữa 2 khung → cân bằng giữa loại trùng thật và giữ khung khác nhau.

**Phụ thuộc tốc độ xe**:
- Xe chạy nhanh (drive B, ~70 km/h): ít khung trùng hơn với ngưỡng 0 (chỉ 8)
- Xe chạy chậm (drive A, ~34 km/h): nhiều khung trùng hơn (61 khung trùng với ngưỡng 0) vì camera đứng hình và tốc độ thấp

## Bước 4 · Gold

### Câu 6
**Kết quả thí nghiệm leakage**:
- Chia theo **khung hình**: 26/26 trip bị chia cắt, tất cả 26 trip có khung ở cả train lẫn test
- Chia theo **trip** (pipeline dùng): 0 trip bị chia cắt

**Tại sao điểm kiểm thử đẹp giả tạo**:
Khi chia ngẫu nhiên theo khung hình, các khung liền nhau (cách nhau 500ms) gần như giống hệt nhau. Nếu một nửa ở train và nửa ở test, mô hình huấn luyện sẽ "thấy" đáp án trong train và nhận ra trong test → điểm cao nhưng không phản ánh khả năng thật.

Chia theo trip đảm bảo **tất cả khung của một cảnh** chỉ ở một split.

### Câu 7
**Tại sao `dataset_version` là băm nội dung**:
- Không phụ thuộc thời gian chạy: hai lần chạy cùng dữ liệu cho cùng hash
- Phát hiện thay đổi: chỉ cần so sánh hash thay vì so sánh từng dòng
- Không thể giả mạo: không thể thay đổi ngày giờ để có cùng version

**Hai lần chạy liên tiếp**: cho kết quả **giống hệt** (cùng hash) vì cùng nội dung.

**Tại sao khác bạn bè**: MSSV được dùng làm salt trong hàm hash chia split. Mỗi MSSV cho một cách xếp trip khác nhau → nội dung gold khác nhau → hash khác nhau.

### Câu 8
**Lineage trong manifest.json**: Dataset được tạo từ:
- Video: `drive_a.mp4` (sha256: `636ba1a9...`), `drive_b.mp4` (sha256: `bfa62703...`)
- Telemetry: `telemetry_a.csv` và `telemetry_b.csv`
- Bảng silver: `silver.frames_flagged`, `silver.frame_telemetry`, `silver.trips`
- Thời điểm tạo: `2026-10-12T14:26:...`

**Thông tin tái tạo sau 3 tháng**:
- `dataset_version`: `072ce76c605a` (băm SHA256 của nội dung gold)
- Video sha256: để verify video gốc
- Table versions: để biết phiên bản silver nào đã dùng
- MSSV: `2A202602711` (để tái tạo cùng split)

## Bước 5 · Điều phối và phục vụ

### Câu 9
**Trip của tôi: trip-05297**

Khi chạy với `LAB_FAIL_ONCE=trip-05297`:
1. Tất cả bronze tasks → xanh
2. Tất cả silver telemetry → xanh
3. `silver_frames_trip-05297` → **đỏ** (RuntimeError: simulated crash)
4. 25 task `silver_frames` còn lại → **upstream_failed** (vì phụ thuộc vào task bị sập)
5. Các task phía sau → không chạy

**Xử lý**:
- Clear **chỉ task đỏ** `silver_frames_trip-05297`
- Task đó chạy lại → xanh
- Các task downstream tự động chạy lại

**Dữ liệu có nhân đôi không? KHÔNG**
- Delta overwrite theo partition nên khi chạy lại trip-05297, chỉ partition của trip đó được ghi đè
- Không ảnh hưởng các partition khác

**dataset_version có đổi không? KHÔNG**
- Vì nội dung gold giống hệt (cùng trip, cùng frames)
- Hash không đổi: `072ce76c605a`

### Câu 10
**Ba thành phần vỡ đầu tiên khi nâng lên 500 xe/ngày**:

1. **MinIO (Object Storage)**:
   - Vấn đề: 500 xe × 2 video × ~15MB × 5 phút = 75 GB/ngày; metadata trong MinIO quá tải
   - Giải pháp: dùng cloud S3 (AWS S3, GCS) với bucket policies, lifecycle rules, và multipart upload cho video lớn (theo slide 22)

2. **Airflow (Orchestration)**:
   - Vấn đề: 500 xe × 26 partitions = 13,000 tasks DAG; LocalExecutor không đủ
   - Giải pháp: chuyển sang KubernetesExecutor hoặc CeleryExecutor, dùng task groups và dynamic task mapping hiệu quả hơn (theo slide 26)

3. **DuckDB (Transformation)**:
   - Vấn đề: DuckDB single-node không scale cho 500 xe × 26 trips × nhiều frames
   - Giải pháp: chuyển sang Spark hoặc dbt với warehouse lớn hơn (Databricks, Snowflake) (theo slide 23)

## Câu hỏi riêng của bạn trên Metabase

**Câu SQL**:
```sql
SELECT 
  trip_id,
  COUNT(*) as total_frames,
  COUNT(*) FILTER (WHERE is_black) as black_frames,
  ROUND(COUNT(*) FILTER (WHERE is_black) * 100.0 / COUNT(*), 2) as black_pct
FROM silver.silver_frame_telemetry
GROUP BY trip_id
ORDER BY black_pct DESC
LIMIT 10
```

**Điều nó cho biết**: Top 10 trip có tỷ lệ khung đen cao nhất. Trip nào có >5% khung đen cần được kiểm tra camera. Drive A có nhiều khung đen hơn (6 vs 4) do điều kiện ánh sáng.

## Nhìn lại

- **Bất ngờ nhất**: cổng chất lượng `max_gap_ms` là `warn` chứ không phải `error` - thiết kế có chủ đích để pipeline không dừng khi có brief sensor dropout.
- **Chưa rõ**: tại sao drive B có nhiều khung tối hơn drive A (61 vs 4) dù tốc độ trung bình B cao hơn; có thể do điều kiện thời tiết/địa hình khác nhau.
- **Lỗi pipeline chưa bắt**: nếu video bị corrupt ở giữa (không phải đen hoàn toàn), pipeline có thể không detect và tạo ra frames không hợp lệ.
