# Lab 01 - Hadoop MapReduce

## Cấu trúc mã nguồn

* **`run.sh`**: Script tự động gọi Maven để build project ra file `.jar` và chạy lần lượt 4 task trên Hadoop LocalRunner.
* **`Lab01/src/main/java/task[1-4]`**: Mã nguồn chính của 4 bài tập (mỗi thư mục chứa cấu trúc Mapper, Reducer và Driver tương ứng).
* **`Lab01/src/main/java/utils`**: Thư mục chứa các class tiện ích:
    * `Parse.java`: Hỗ trợ xử lý và làm sạch dữ liệu thô (xóa khoảng trắng thừa...).
    * `SideTables.java`: Đưa các file dữ liệu phụ (như `movies.txt`, `users.txt`) vào Distributed Cache để xử lý mapping.

## Hướng dẫn chạy (Môi trường WSL)

**Lưu ý:** Chạy Hadoop LocalRunner trực tiếp trên thư mục Windows mount sang WSL rất dễ sinh ra lỗi permission hệ thống NTFS (như lỗi `ExitCode=1 chmod`). Để code chạy ổn định, cần đưa project vào hẳn môi trường file system của Linux.

Các bước thực hiện trên WSL Ubuntu:

**Bước 1:** Khởi động môi trường Ubuntu Mở terminal và gọi lệnh để vào bản distro đã cài sẵn JDK và Maven.
```bash
wsl -d Ubuntu
```
**Bước 2:** Chuyển project vào Linux Copy toàn bộ thư mục từ ổ Windows (ví dụ ổ D) vào thư mục home của Linux để tránh lỗi quyền truy cập.
```bash
rm -rf ~/thuc_hanh && cp -r /mnt/d/code_nam_3/big_data/thuc_hanh ~/thuc_hanh
```
**Bước 3:** Xử lý định dạng dòng và chạy script Code lưu từ Windows hay bị thừa ký tự \r ở cuối dòng, làm file script lỗi trên Linux. Dùng sed dọn sạch trước khi chạy.
```bash
cd ~/thuc_hanh
sed -i 's/\r$//' run.sh
bash run.sh
```
**Bước 4:** Lấy kết quả về lại Windows Copy 4 file output từ Linux trả về ổ cứng Windows để nộp bài.
```bash
cp ~/thuc_hanh/Lab01/output/output_task_*.txt /mnt/d/code_nam_3/big_data/thuc_hanh/Lab01/output/
```
