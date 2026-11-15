# Analisis ACC-01: jumlah peluncuran per roket (DWH vs statistik LL2)

**Pemeriksaan.** `sql/validation/dw_validation.sql` ACC-01 (severity WARN) membandingkan jumlah baris
`dw.fact_peluncuran` per roket aktif dengan atribut `total_peluncuran` dari snapshot terakhir
`dw.fact_konfigurasi_roket` (statistik resmi LL2 `/launcher_configurations/`). Roket dianggap tidak cocok bila
selisihnya lebih dari 10%. Hasil per 4 November 2026: **15 dari 24** roket aktif (≥ 20 peluncuran) berselisih.

Pemeriksaan ini sengaja diberi severity WARN, bukan ERROR, karena kedua angka berasal dari sumber yang berbeda.
Tujuannya mengukur *veracity* integrasi sumber, bukan menahan load.

## Rincian (snapshot 2026-11-04)

| Roket | Statistik LL2 | DWH | Selisih | Penyebab |
|---|---:|---:|---:|---|
| Falcon 9 Block 5 | 644 | 644 | 0,0% | cocok |
| Kuaizhou 1A, Long March 6A, Rokot/Briz KM, Soyuz | - | - | 0,0% | cocok |
| Pegasus XL, PSLV-XL, Long March 2C, 2F/G | - | - | < 10% | cocok (dalam toleransi) |
| Electron | 99 | 72 | −27,3% | A. granularitas varian |
| Proton-M/Briz-M | 68 | 15 | −77,9% | B. arsip tidak lengkap |
| Proton-M | 48 | 4 | −91,7% | A + B |
| SpaceShipTwo | 67 | 12 | −82,1% | C. perbedaan cakupan |
| Long March 2D | 101 | 84 | −16,8% | B |
| Long March 3B | 24 | 11 | −54,2% | A + B |
| Long March 3A, 4B, 4C | 27 / 59 / 60 | 17 / 43 / 45 | −25% s.d. −37% | B |
| Soyuz 2.1a, 2.1b, 2.1a/Fregat-M, 2.1b/Fregat(-M) | - | - | −14% s.d. −55% | A + B |

## Penyebab

**A. Granularitas varian roket berbeda antar sumber.** Arsip internal (Maven) mencatat roket sampai tahap atas
(*upper stage*), sedangkan LL2 sering menggabungkannya dalam satu konfigurasi. Contoh paling jelas adalah Electron.
Di arsip tercatat `Electron` 1, `Electron/Curie` 24 dan `Electron/Photon` 3 (2017–Jul 2022), ditambah 71 peluncuran
LL2 sejak Agustus 2022. Totalnya 1 + 24 + 3 + 71 = 99, persis sama dengan statistik LL2. Datanya lengkap; yang
berbeda hanya penamaannya.

**B. Arsip internal tidak lengkap untuk sebagian roket Rusia dan Tiongkok.** Contohnya, `space_missions.csv` hanya
memuat 14 peluncuran Proton-M/Briz-M (2016–2021), padahal roket ini terbang sejak 2001. Kekurangan ini berasal dari
dataset sumber (ditelusuri langsung di CSV), bukan dari baris yang ditolak pipeline. `staging.stg_reject` hanya
memuat 1 baris arsip, yaitu duplikat identik Long March 2D (5 Nov 2008, Shiyan-3 & Chuangxin-1(02), aturan TC-01).
Karena itu CSV berisi 44 baris Long March 2D dan DWH 43, dan selisih ini memang benar.

**C. Perbedaan cakupan definisi peluncuran.** LL2 menghitung seluruh penerbangan SpaceShipTwo, termasuk uji suborbital
dan *glide flight*, sedangkan arsip dan endpoint `/launches/previous` yang dipakai hanya memuat sebagian.

## Keputusan

1. **Tidak memaksa angka agar cocok.** Menggabungkan `Electron/Curie` ke `Electron` akan menghapus informasi
   tahap atas yang dipakai model ML (riwayat per roket). Selain itu, aturan TI-03 (UTS) memetakan nama LL2 ke nama
   baku arsip, bukan sebaliknya. Data asli tetap utuh dan selisihnya terdokumentasi.
2. **Pemetaan nama perusahaan diperbaiki** untuk padanan yang pasti (data steward, `mappings/map_perusahaan.csv`):
   Royal Aircraft Establishment → RAE, European Launcher Development Organisation → CECLES, National Center of
   Space Research → CNES, Ministry of Defence of the Russian Federation → VKS RF, Makeyev Rocket Design Bureau → SRC,
   Energia → RKK Energiya. Hasilnya `dim_perusahaan` turun dari 117 ke 111 entitas dan 6 nama di
   `meta.review_queue` berstatus MAPPED. Nama yang ambigu sengaja tidak dipetakan, misalnya *Israeli Space Agency*
   (di arsip `ISA` berarti Iranian Space Agency).
3. **Rekomendasi.** Tambahkan sumber historis LL2 sebelum Agustus 2022 (`/launches/previous` tanpa batas tanggal,
   ±7.000 record ≈ 70 request = ±5 jam kuota) agar arsip yang kurang dapat dilengkapi. Ini direncanakan sebagai
   pengembangan berikutnya.

## Review queue (TI-05)

Setelah pemetaan: 47 perusahaan, 292 roket dan 14 lokasi berstatus PENDING. Sebagian besar wajar karena berupa
entitas yang memang baru muncul setelah 2022 (Orienspace, Space One, Isar Aerospace, Space Pioneer, Innospace,
Gilmour, Skyroot, dll.) atau konfigurasi historis LL2 yang tidak pernah terbang di periode arsip. Entitas tersebut
tetap dimuat ke DWH dengan nama aslinya (tidak dibuang), sesuai aturan TI-05.
