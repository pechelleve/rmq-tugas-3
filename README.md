# Tugas 3: Work Queues dan Broadcast dengan RabbitMQ (C# .NET 8)

Implementasi modul *Messaging Service menggunakan RabbitMQ* bagian 2.1 dan 2.2 dengan RabbitMQ.Client 6.8.1.
Bagian 2.3 (Message Filtering, opsional) belum dikerjakan.

| Project | Peran | Modul |
|---|---|---|
| `Tasker` | Publisher untuk work queue. Mengirim angka ke queue worker | 2.1.1 |
| `Worker-cmd` | Consumer work queue. Menghitung mundur 1 detik per angka | 2.1.2 |
| `Broadcaster` | Publisher broadcast. Mengirim pesan ke exchange fanout | 2.2.1 |
| `Receiver` | Consumer broadcast. Menerima pesan dari exchange fanout | 2.2.2 |

## Prasyarat
- RabbitMQ berjalan di `localhost:5672` (user `guest` / `guest`).
- .NET 8 SDK.

## Menjalankan

**2.1 Work queue:** satu Tasker, dua Worker.
1. Jalankan Worker di dua terminal: `dotnet run --project Worker-cmd`, lalu isi nama queue, misalnya `workQ`.
2. Jalankan Tasker di terminal lain: `dotnet run --project Tasker`. Isi tujuan `workQ`, lalu angka seperti `10,21,5,7,3`.

**2.2 Broadcast:** satu Broadcaster, dua Receiver.
1. Jalankan Receiver di dua terminal: `dotnet run --project Receiver`, lalu isi nama exchange, misalnya `logs`.
2. Jalankan Broadcaster: `dotnet run --project Broadcaster`. Isi tujuan broadcast `logs`, lalu pesan.

Ketik `exit` pada pesan untuk keluar dari Tasker atau Broadcaster. Tekan Enter pada Worker atau Receiver untuk memutus koneksi.

## Perbedaan dengan kode modul

| Bagian | Modul | Di project ini | Alasan |
|---|---|---|---|
| Tasker dan Broadcaster | Universal Windows App dengan form | Console App | Konsisten dengan Tugas 2, dan bisa dijalankan tanpa Visual Studio |
| Isi pesan | `body` langsung | `ea.Body.ToArray()` | `Body` bertipe `ReadOnlyMemory<byte>` di RabbitMQ.Client 6.x |
| Exchange broadcast | Dideklarasikan di `CreateRMQChannel` | Dideklarasikan di `SendMessage` | Supaya exchange bisa diganti antara A dan B tanpa membuat ulang channel |
| Channel Receiver | `using (channel = ...)` | Dibuka sekali, ditutup di `Disconnect()` | Menghindari channel ditutup dua kali |
| Tasker | `mandatory` tidak ada | `mandatory: true` dengan event `BasicReturn` | Publisher menampilkan peringatan jika queue tujuan tidak ada |

## Catatan perilaku
- **Work queue dengan `autoAck: true`** (sesuai modul): pesan langsung dihapus begitu dikirim ke worker. Jika worker mati di tengah hitungan, pesan yang sedang diproses ikut hilang. Untuk pemrosesan yang tidak boleh hilang, gunakan manual ack dan `BasicQos` (prefetch 1).
- **Distribusi work queue** bersifat round-robin, bukan berdasarkan beban. Worker yang sedang sibuk tetap menerima pesan berikutnya.
- **Broadcast**: setiap Receiver memiliki queue anonim sendiri yang eksklusif. Queue ini hilang saat Receiver disconnect, sehingga pesan yang dikirim saat tidak ada Receiver terhubung tidak tersimpan.
