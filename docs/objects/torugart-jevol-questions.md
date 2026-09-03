# Jevol JLS-130/150 — второй раунд вопросов поставщику (03.09.2026)

> Контекст: кандидат в габаритные датчики для «Торугарта» (см. `torugart.md`, раздел «Кандидат
> в габаритные датчики»). Первый раунд (6 вопросов, 26.08.2026) закрыл скорость (3–7 км/ч —
> совместимо) и вскрыл ANPR как обязательную опцию (+$980), но ДВА блокера остались:
> (1) интерфейс выдачи данных — «поддерживается», документации нет; (2) −20 °C без подогрева.
> Игорь пишет поставщику напрямую; по ответам даём заключение «устраивает / не устраивает».
> Статус: **письмо подготовлено, ответов ещё нет.**

## Как отправлять

- Лучше письмом на e-mail (спросить у менеджера Alibaba рабочий e-mail), а не в чат: 27 пунктов
  в чате потеряются. Просить отвечать **под теми же номерами** и прикладывать документы.
- Назвать срок ответа (например, 7 дней) и прямо сказать, что решение о покупке зависит от ответов —
  так отвечают полнее.
- Параллельно (не в этом письме) запросить у Тензо-М цену их лазерного сканера габаритов из СОДИ
  (исполнение до −40, интегрирован в «Статику 3.0») — для сравнения.

## Письмо (EN, готово к отправке)

```text
Subject: JLS-150 Vehicle Dimension Laser Scanning System – technical questions before purchase (customs checkpoint, Kyrgyzstan)

Dear <manager name>,

Thank you for your earlier answers about the JLS-130/150. We are now preparing the purchase decision and need precise answers to the questions below. Our decision depends directly on these answers. Please reply point by point under the same numbers and attach the requested documents. If some function does not exist today, please say so directly – this is better for both of us than a general "yes".

Project background (important for your answers):
- Site: Torugart border crossing (China–Kyrgyzstan border), Kyrgyzstan. Outdoor installation at about 3,500 m altitude; winter temperatures down to -35...-40 °C, snow, strong wind, strong sun.
- The scanner will work together with in-motion axle weighing scales (weighing at 3-8 km/h). Vehicles: mainly heavy trucks and road trains (truck + trailer) up to 20-25 m long, with Kyrgyz, Chinese and Kazakh license plates.
- The measurement results must be received automatically by OUR OWN software (our weighing system) and forwarded to the customs IT system. The scanner must work unattended, 24/7, without an operator.

A. DATA OUTPUT TO THIRD-PARTY SOFTWARE (most important)

1. In the CURRENT software version, how can external software receive the measurement results? Please mark everything that exists today (not "can be developed"):
   (a) TCP/IP socket protocol; (b) HTTP/REST API; (c) direct database access (which DBMS? is the table structure documented?); (d) automatic file export (CSV/XML/JSON) to a folder; (e) serial port RS-232/485; (f) other.
2. Please send the interface documentation (an existing Chinese-language document is acceptable) and a REAL sample of one measurement record with all fields (record ID, date/time, plate number, length, width, height, wheelbase, number of axles, status/flags, photo file names, etc.).
3. Is the result sent out automatically when the vehicle leaves the scanning zone (push), or must our software request it (poll)? How many seconds after the vehicle exits is the result available?
4. Does every measurement have a unique ID and a timestamp? Are photos and raw scan profiles stored? Where and for how long?
5. Does the software need internet access, a cloud service, a license server or a USB dongle? Which Windows version is on the supplied PC? Can our software run on the same PC, or must it be a separate PC connected by LAN?
6. Before purchase, can you show us in a remote session (TeamViewer/AnyDesk) how an external program receives a measurement result? Can the interface documentation be listed in the contract and delivered before the final payment?
7. If any interface must be developed or customized for us: is it included in the USD 19,000 price? If not, please quote the price and the development time.

B. COLD CLIMATE (second critical point)

8. Which laser scanner is used – manufacturer and exact model (for example SICK, Hokuyo, Pepperl+Fuchs, Wanji, LSLIDAR, other)? Please attach the scanner datasheet.
9. Your stated operating range is -20...+70 °C. Which component limits -20 °C: the scanner itself, the power supply, the control box or the PC?
10. Do you offer, or have you already delivered, a low-temperature version for -40 °C: heated scanner housings / heated windows, heated control cabinet, industrial PC? Please quote price and delivery time. Do you have JLS installations in cold regions (Heilongjiang, Inner Mongolia, Xinjiang, Mongolia, Russia, Kazakhstan)? Can you give a reference?
11. If we install our own heated protective housings around the scanners, does the warranty remain valid? Which window material is transparent for your scanners' laser wavelength?
12. Snow, frost or dirt on a scanner window, fog, blowing snow, direct sun: does the software detect a bad measurement and mark the record as invalid, or can it output wrong dimensions without any warning? Is a device status / self-diagnostics available to external software?
13. Power consumption of the whole system; can it run from a UPS; after a power failure, does the system restart and continue automatically without an operator?

C. UNATTENDED OPERATION, ANPR AND VEHICLE SEPARATION

14. Please confirm: without the ANPR option an operator must finish every pass manually; with ANPR (USD 980) the cycle is fully automatic – the vehicle enters, is scanned, the result is saved and sent out, and the system is ready for the next vehicle – without any operator action.
15. ANPR: which countries / plate formats are supported – Kyrgyzstan, Kazakhstan, China, Russia, Uzbekistan? Can new plate templates be added? If a plate is not recognized, is the measurement still saved and sent out (with an empty plate field), or is it discarded? Can our software send the plate number / vehicle ID into your system instead?
16. How does the system detect the beginning and the end of a vehicle (the scanners themselves, a photocell, an inductive loop)? Is a truck with a trailer (road train) measured as ONE vehicle? What is the minimum distance between two vehicles? What happens if a vehicle stops inside the zone or reverses?
17. Vehicles longer than 20 m (road trains, oversize cargo): what does the system output – an error flag, a value cut at 20 m, or the correct length? Is a longer range (25 m) available?
18. What exactly is measured: the outline of the vehicle including the load (tarpaulin, protruding cargo), or the body only? How are mirrors and antennas handled? Are wheelbase(s) and the number of axles output for every vehicle?

D. ACCURACY AND CERTIFICATES

19. Please clarify "accuracy 1 mm, error ±0.8 %": is ±0.8 % the guaranteed error for length, width and height at 3-7 km/h? Do you have a third-party test report? How is the system calibrated on site (reference vehicle, calibration frame), and how often must it be recalibrated?
20. Which certificates exist (CE, ISO 9001, others)? Is the system type-approved as a measuring instrument in any country (China CPA, OIML, other)? What exactly is the certificate you offered to arrange at our cost?

E. SCOPE OF SUPPLY AND COMMERCIAL TERMS

21. Which exact model is offered for USD 19,000 – JLS-130 or JLS-150? What is the difference? Software version and interface language (English / Russian?).
22. Full bill of materials: number of scanners, control box, PC (specification), monitor, cables (what distance between the two posts and from the posts to the control room is possible?), mounting kit, software licenses. Does USD 980 include the ANPR camera, its lighting and the license?
23. Frame drawings for local fabrication: post dimensions, foundation, lane width, mounting height; minimum straight road length before and after the scanner; can the posts be placed directly before or after a 3 × 6 m in-motion weighing platform?
24. Installation, calibration and training: remote guidance only, or an engineer on site (cost)? Documentation and software UI in English / Russian?
25. Warranty period and conditions; price and availability of a spare scanner; software updates; remote support hours (time zone).
26. Delivery: lead time, Incoterms and shipping cost to Bishkek, Kyrgyzstan; packing weight and volume; HS code; payment terms.
27. Reference installations at border crossings, customs or weigh stations (not vehicle inspection stations): how many, and can we contact one of them?

Please also send: the scanner datasheet, the software user manual, the interface documentation, a sample data record, frame drawings, certificates, and an updated commercial offer including JLS-150 + ANPR + low-temperature option + drawings + shipping to Bishkek.

We are ready to proceed quickly if the answers are positive. We would appreciate your reply within 7 days.

Best regards,
<name>
<position>, GTI OJSC (Kyrgyzstan)
<phone / e-mail>
```

## Ключ к вопросам (что проверяем и какой ответ закрывает пункт)

| Блок | Устраивает, если | Стоп-фактор (не устраивает) |
|---|---|---|
| A. Интерфейс данных (1–7) | Существует СЕГОДНЯ хотя бы один задокументированный канал: TCP/HTTP-протокол, БД с описанной структурой или файловая выгрузка с описанным форматом; прислан реальный образец записи с полями; результат приходит сам или доступен запросом в течение ~30 с после проезда; есть уникальный ID и время; документация — в контракте до финального платежа; демо по удалёнке проведено | «Разработаем после покупки»; образца записи нет; результат только на экране/в печати; работа только через облако/интернет; доработка интерфейса за отдельные деньги без указания срока |
| B. Мороз (8–13) | Назван промышленный лидар с паспортным диапазоном −30/−40 (тогда −20 — ограничение шкафа/ПК, лечится утеплённым шкафом) ИЛИ Jevol предлагает низкотемпературное исполнение с ценой и сроком ИЛИ есть референс в холодном регионе; ПО помечает плохие измерения (обмерзание/грязь), а не молча выдаёт неверные размеры; после пропадания питания стартует само | Модель лидара не раскрывают и подогрева нет; «ставьте кожухи сами» с потерей гарантии — только как осознанный риск; ПО не отличает плохое измерение от хорошего (для безоператорного поста на перевале это критично) |
| C. Автономность и ANPR (14–18) | С ANPR цикл полностью автоматический; нераспознанный номер НЕ отбрасывает измерение (запись сохраняется с пустым номером) либо номер можно подать из нашей системы; поддержка номеров КР/КЗ/КНР или добавление шаблонов; автопоезд считается одной машиной; ТС длиннее 20 м даёт помеченный результат, а не «обрезанные» 20 м | Измерения без номера теряются; автопоезд режется на два ТС; длина молча обрезается до 20 м |
| D. Точность и статус (19–20) | ±0.8 % подтверждена как гарантированная погрешность; понятна процедура калибровки на месте | Не блокер: сертификат соответствия ≠ реестр СИ КР. Для таможни данные «справочные» — так и фиксировать в контракте с таможней; для санкций за негабарит потребуется своя метрология в КР |
| E. Комплект и условия (21–27) | Итоговая цена ≈ $19 000 + $980 + низкотемпературное исполнение + доставка; гарантия ≥ 12 мес; известна цена запасного лидара; чертежи рамы и требования к площадке даны; есть хоть один референс на границе/весовом посту | Цена «поплыла» без объяснений; запчастей нет; референсов вне станций техосмотра нет (само по себе не отказ, но снижает доверие к автономному режиму) |

**Правило заключения:** блоки A и B — обязательные (любой стоп-фактор там = «не устраивает»
с объяснением причины); блок C — обязателен пункт про потерю измерений без номера и про
автопоезд; D и E — влияют на цену и условия, но не на «да/нет».

## После ответов

1. Ответы (с приложениями) — в переписку и сюда, в раздел «Ответы поставщика (раунд 2)».
2. Заключение по таблице выше: «устраивает при условиях …» либо «не устраивает, потому что …».
3. Если «устраивает» — в контракт: документация интерфейса и образец данных до финального
   платежа, низкотемпературное исполнение, ANPR, чертежи рамы, гарантия и запасной лидар.
