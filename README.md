Cấu trúc mã nguồn
run.sh: Script tự động gọi Maven để build project ra file .jar và chạy lần lượt 4 task trên Hadoop LocalRunner.

Lab01/src/main/java/task[1-4]: Mã nguồn chính của 4 bài tập (mỗi thư mục chứa cấu trúc Mapper, Reducer và Driver tương ứng).

Lab01/src/main/java/utils: Thư mục chứa các class tiện ích:

Parse.java: Hỗ trợ xử lý và làm sạch dữ liệu thô (xóa khoảng trắng thừa...).

SideTables.java: Đưa các file dữ liệu phụ (như movies.txt, users.txt) vào Distributed Cache để xử lý mapping.

Hướng dẫn chạy project (Môi trường WSL)
Lưu ý: Chạy Hadoop LocalRunner trực tiếp trên thư mục Windows mount sang WSL rất dễ sinh ra lỗi permission hệ thống NTFS (như lỗi ExitCode=1 chmod). Để đảm bảo code chạy không lỗi, cần copy project vào hẳn trong môi trường file system của Linux.

Dưới đây là các bước thực hiện trên WSL Ubuntu:

Bước 1: Khởi động môi trường Ubuntu
Mở terminal và chạy lệnh để vào đúng bản distro đã cài sẵn JDK và Maven.

Bash
wsl -d Ubuntu
Bước 2: Copy project vào môi trường Linux
Chuyển toàn bộ thư mục bài làm từ ổ đĩa Windows (ví dụ ổ D) vào thư mục home của Linux để tránh lỗi quyền truy cập.

Bash
rm -rf ~/thuc_hanh && cp -r /mnt/d/code_nam_3/big_data/thuc_hanh ~/thuc_hanh
Bước 3: Xử lý định dạng dòng và chạy script
Code viết trên Windows thường dính ký tự \r ở cuối dòng, làm file .sh không chạy được trên Linux. Cần dùng sed để fix trước khi thực thi.

Bash
cd ~/thuc_hanh
sed -i 's/\r$//' run.sh
bash run.sh
Hệ thống sẽ chạy các job MapReduce. Khi terminal in ra Done. Reports: output_task_1.txt ... tức là quá trình xử lý đã xong.

Bước 4: Lấy file kết quả về lại Windows
Copy 4 file output từ Linux trả ngược về lại thư mục gốc trên ổ cứng Windows để nộp bài hoặc xem kết quả.

Bash
cp ~/thuc_hanh/Lab01/output/output_task_*.txt /mnt/d/code_nam_3/big_data/thuc_hanh/
