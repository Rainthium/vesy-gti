# Jevol JLS-130/150 — второй раунд вопросов поставщику (03.09.2026)

> Контекст: кандидат в габаритные датчики для «Торугарта» (см. `torugart.md`, раздел «Кандидат
> в габаритные датчики»). Первый раунд (6 вопросов, 26.08.2026) закрыл скорость (3–7 км/ч —
> совместимо) и вскрыл ANPR как обязательную опцию (+$980), но ДВА блокера остались:
> (1) интерфейс выдачи данных — «поддерживается», документации нет; (2) −20 °C без подогрева.
> Игорь пишет поставщику напрямую; по ответам даём заключение «устраивает / не устраивает».
> Статус: **ответы получены 13.09.2026 (все 27 пунктов + КП + ISO 9001 + руководство V1.5),
> заключение — «условно устраивает» (раздел «Ответы поставщика (раунд 2)» ниже).
> Ждёт: письмо №3 (условия до аванса) и решение руководства о закупке.**
> Страница-заключение для руководства (артефакт, приватная ссылка):
> https://claude.ai/code/artifact/d3e6ba62-9de3-4bd7-87cf-cd11efa266b8

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

## Ответы поставщика (раунд 2) — 13.09.2026

Получено четыре файла (Downloads Игоря): `answers.pdf` — ответы под нашими номерами 1–27
(по пунктам, без «воды», честно называют, чего нет); `Quotation JEVOL26090811 … .pdf` — КП
(в документе № JEVOL26211490 от 29.04.2026 при сроке действия 3 мес — дата явно из шаблона,
просить переиздать); `ISO-EN.pdf` — ISO 9001:2015 на **Nantong** Jevol (производственная
площадка, Хаймэнь; в КП юрлицо — **Shanghai** Jevol), область «оборудование для СТО», до
30.01.2029, орган ZhongTai Union (CNAS); `JLS-150 … Instruction V1.5 2025-01-20.pdf` —
руководство по установке и ПО (23 стр). Паспорт лидара **SICK PICS150-01000 Prime-1**
(picoScan150, арт. 1134609, datasheet sick.com от 17.08.2026) скачан и сверен: цифры
поставщика совпадают с паспортом.

Что подтверждает руководство: 3 лидара — №1 и №2 на поперечине портала (ширина и высота),
№3 на отдельной стойке в 17–20 м от портала, смотрит вдоль полосы (длина); стойки портала
4,9 м, полоса 4 м, линия сканирования 20 м; в комплекте 2 фотодатчика (триггер), шкаф
220 В → 24 В с коммутатором, промышленный ПК; ПО заточено под СТО: «New Order» → ввод
данных ТС → «Testing» → результат «Pass»/печать/«Upload» (загрузка на сервер существует,
протокол не описан); скорость проезда «менее 5 км/ч» (п. 4.1.4) при спецификации 3–7;
предостережения: шкаф не на солнце и не во влажном месте, молниезащита обязательна,
сварка на раме после установки лидаров запрещена.

### Итог по ключу

| Блок | Итог | Что ответили (суть) | Чем закрыть |
|---|---|---|---|
| A. Интерфейс (1–7) | 🟡 условно | Сегодня — только выгрузка файлов в папку (вариант d); результат «push или файл» ≤ 5 с после выезда; уникальный ID + время есть, «фото» хранятся; документации и образца записи НЕТ («данные на компьютерах клиентов» — слабое оправдание, демо-стенд у них есть); только LAN, активация кодом, без интернета/облака/USB-ключа, **наше ПО можно на тот же промышленный ПК**; демо по удалёнке — да; документация в контракт до финального платежа (= 70 % до отгрузки) — да; доработки **$50/ч, ≤ 1 недели** | Образец файла + описание всех полей с их демо-стенда и демо по удалёнке ДО аванса; формат и доработки — приложением к контракту |
| B. Мороз (8–13) | 🟡 условно, риск остаётся | Лидар SICK picoScan150 Prime-1: работа **−33…+50 °C**, хранение −40…+70, IP65/67, 905 нм, класс 1, 4,5 Вт, высота < 5 000 м, засветка 100 клк (паспорт подтверждает); шкаф/ПК/БП — выше −20 °C, «лучше в помещении»; низкотемпературной версии и референсов в холоде НЕТ; «по данным SICK саморазогрев, теоретически −40» — в паспорте этого нет; свой обогрев — потеря гарантии на повреждения от него, окно перед лазером ставить нельзя, козырёк от дождя/снега в комплекте, при сильном снеге — навес над полосой; **ПО не помечает плохие измерения** (снег/иней/грязь/туман → неверные размеры без предупреждения), могут добавить сигнал «лидар перекрыт»; < 300 Вт, ИБП, автостарт «можно реализовать» | Шкаф и ПК — в тёплую весовую (Ethernet ≤ 100 м, 24 В — запас по 9–30 В); навес над рамой/полосой; доработка «статус устройства + флаг качества в каждой записи»; автостарт — в контракт; наши проверки правдоподобия; принять риск ночей ниже −33 °C; данные — справочные |
| C. Автономность (14–18) | 🟡 не до конца | Без ANPR — оператор оформляет каждую машину вручную; ANPR делали только для одной страны, рекомендуют камеру купить нам, они интегрируют; «поддерживаем запись номера извне»; **опция ANPR $980 из КП исчезла**; **НЕ ответили, теряется ли измерение без номера**; триггер без ANPR — сам лидар; автопоезд = одно ТС ✓; интервал между машинами 30 м; остановка/реверс до выхода хвоста не мешают, долгая остановка/многократный реверс «может подвесить систему»; исполнение 25 м есть, > 25 м — флаг ошибки ✓ (цена не названа); контур с грузом/тентом измеряется как есть, зеркала/антенны игнорируются; оси/база — только с доп. фотодатчиками, в снег неточно | Письменно: автоцикл без ANPR с номером от нашей системы или пустым, измерение сохраняется и выгружается всегда; авто-сброс цикла по таймауту; цена и требования площадки для опции 25 м (третья стойка в 25 м, дальность лидара при 100 клк по тёмной цели 16–23 м — чем обеспечивают) |
| D. Точность (19–20) | 🟢 как ожидали | ±0,8 % гарантированно по Д/Ш/В при 3–7 км/ч (20 м → ±16 см, 2,55 м → ±2 см, 4 м → ±3 см); стороннего протокола нет; калибровка эталонным ТС/рамой (по руководству 10–20 проездов на 5 км/ч), раз в год; только ISO 9001; CPA в КНР не требуется; CNAS-калибровка за наш счёт (это «прибор проверен в лаборатории», не утверждение типа) | Данные для таможни — справочные (в договоре); калибровка своими силами по руководству |
| E. Условия (21–27) | 🟡 уточнить | Только JLS-150; UI EN, RU «при нашей проверке языкового файла»; комплект: 3 лидара, шкаф, промышленный ПК, кабели по объекту, ПО без подписки (**монитор не упомянут**); чертежи «могут быть предоставлены» (не приложены); монтаж/калибровка/обучение — только удалённо, инженера нет; гарантия 12 мес с ввода / 14 с отгрузки, ремонт бесплатно, доставка наша; запасной лидар **$5 500**, срок «уточнить»; обновления ПО бесплатны; поддержка GMT+8, ответ 8 раб. ч; 60 дней после аванса; Incoterm «уточняется» (в КП «CIF Shanghai» рядом с «доставка Шанхай–Бишкек $1 000 без растаможки» — противоречие); 80 кг / 0,2 м³; HS 9031809090; **30 % аванс / 70 % до отгрузки**; референсов на границах/весовых постах НЕТ — только СТО | Переиздать КП (дата и срок, доставка до Бишкека и кто растаможивает, монитор, срок запасного лидара, опции); чертежи — сейчас; юрлицо контракта (Shanghai vs Nantong) |

**Цена по КП**: JLS-150 $17 500 + доставка $1 000 = **$18 500** (в чате было $19 000);
опции: рама $1 500 (или своя по чертежам), запасной лидар $5 500. Реальный бюджет
оборудования: от $21,5 тыс. (без запасного лидара и камеры) до ≈ $28 тыс. (с запасным лидаром,
доработками ≈ $1–2 тыс., своей ANPR-камерой ≈ $0,5–1,5 тыс.) + опция 25 м (цена не названа)
+ местные работы (фундаменты двух стоек и третьей в 20–25 м, навес, кабели, ИБП, молниезащита)
+ пошлины/НДС КР.

### Заключение

**«Условно устраивает».** По строгому ключу сегодня два стоп-фактора в обязательных блоках:
A — нет образца записи и документации; B — ПО не отличает плохое измерение от хорошего.
Оба закрываются силами поставщика до аванса (образец с демо-стенда — день работы; флаг
качества — доработка по $50/ч), и поставщик в принципе согласен (документация в контракт,
диагностику «можно добавить»). Условия 5–7 и модель лидара — лучше ожиданий (наше ПО на их
ПК, без интернета, SICK с паспортом до −33, а не до −20). Остаточный риск, который не снимет
никто: ночи ниже −33 °C (лидар вне паспорта; хранение до −40 → не сломается, но работа не
гарантирована) и обмерзание колпака в метель → неверные размеры без предупреждения. Меры:
навес, тёплое помещение для шкафа, флаг качества + наши проверки правдоподобия (сверка
с классом ТС и осями от весов, фото), данные для таможни — справочные, запасной лидар
на складе. Референсов на границах нет — мы первые; их ПО заточено под СТО, автономность
дорабатывать вместе с ними. Решение о покупке на этих условиях — за руководством;
Тензо-М (сканер СОДИ до −40, интегрирован в «Статику») — сравнить, если ответ получен.

Что делаем мы после покупки (объём небольшой, после образца формата): модуль на их
промышленном ПК читает папку выгрузки (или принимает push), разбирает запись, сопоставляет
с проездом по времени (±секунды) и номеру, кладёт Д/Ш/В + источник в событие таможне
(расширяемость в модели заложена, decisions 20.08/25.08), хранит файл-оригинал, помечает
неправдоподобные значения. Распознавание номеров у нас — только по решению Игоря (CLAUDE.md
«Чего не делать»).

### Письмо №3 (EN, короткое — условия до аванса)

```text
Subject: JLS-150 – items to close before deposit + updated quotation

Dear <manager name>,

Thank you for the detailed answers. We are ready to proceed, subject to the items below.

Before deposit (no cost expected):
1. A sample export file from your demo system (one real measurement record with all fields)
   and a description of every field and of the file naming.
2. A remote session: a vehicle pass on your demo system -> the file appears in the folder ->
   we see its contents.
3. Written confirmation that the cycle runs fully automatically WITHOUT ANPR: the LiDAR
   detects the vehicle, the plate number is written in by our software (or left empty),
   and every measurement is saved and exported regardless of the plate.

Fixed-price quotation (USD 50/h – please state the hours) for:
4. Device status and a quality flag in every exported record (LiDAR blocked / contaminated /
   incomplete scan), plus a status file or heartbeat readable by external software.
5. Automatic reset of a stuck cycle after a timeout, and automatic start after a power
   failure, both without an operator.
6. The 25 m range version: price, site requirements (straight section, distance of the
   length-radar post) and how the range is achieved.

Documents and quotation:
7. Frame and foundation drawings (post dimensions, lane width, mounting height, straight
   section before/after, placement next to a 3 x 6 m weighing platform) – now, before
   the decision.
8. A re-issued quotation: current date and validity; delivery to Bishkek with Incoterm and
   who handles customs clearance; monitor included or not; spare LiDAR price and lead time;
   items 4-6 as options; the contracting entity (Shanghai Jevol or Nantong Jevol, the
   ISO 9001 holder).

Best regards,
<name>, GTI OJSC (Kyrgyzstan)
```
