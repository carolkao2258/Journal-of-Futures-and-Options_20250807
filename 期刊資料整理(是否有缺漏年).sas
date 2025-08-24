/* 建議先開啟訊息，跑穩後可關 */
options notes stimer source nosyntaxcheck;

/* 0) 匯入 */
%let infile=/home/u64061874/patent_regression_data6_announce.xlsx;

proc import out=have_raw
     datafile="&infile"
     dbms=xlsx replace;
     getnames=yes;
run;

/* 1) year 正規化，只保留需要的欄位（companyID, companyName, year, year_num） */
data have;
  set have_raw;

  length year_num 8;
  if vtype(year)='C' then year_num = input(strip(year), ?? best32.);
  else                   year_num = year;

  if year_num < 1900 or year_num > 2100 then year_num = .;

  keep companyID companyName year year_num;
run;

/* 2) 偵測年份範圍 */
proc sql noprint;
  select floor(min(year_num)), ceil(max(year_num))
    into :y_min, :y_max
  from have
  where not missing(year_num);
quit;
%put NOTE: Year range detected = &y_min - &y_max;

/* 3) 年份清單 + 公司清單 */
data years;
  do year_num=&y_min to &y_max; output; end;
run;

proc sort nodupkey data=have out=companies(keep=companyID companyName);
  by companyID;
run;

/* 4) 應有的 company×year */
proc sql;
  create table should_have as
  select c.companyID, c.companyName, y.year_num
  from companies as c, years as y;
quit;

/* 5) 找缺的年 */
proc sql;
  create table have_cy as
  select distinct companyID, companyName, year_num
  from have
  where not missing(companyID) and not missing(year_num);

  create table missing_list as
  select a.companyID, a.companyName, a.year_num as missing_year
  from should_have as a
  left join have_cy as b
    on a.companyID=b.companyID and a.year_num=b.year_num
  where b.year_num is null
  order by companyID, missing_year;
quit;

/* 6) 完整性彙總 */
proc sql;
  create table have_cnt as
  select companyID, count(distinct year_num) as years_present
  from have_cy
  group by companyID;
quit;

%let years_expected = %eval(&y_max - &y_min + 1);

proc sql;
  create table company_completeness as
  select c.companyID, c.companyName
       , coalesce(h.years_present,0) as years_present
       , &years_expected             as years_expected
       , (&years_expected - coalesce(h.years_present,0)) as years_missing
       , case when coalesce(h.years_present,0) = &years_expected
              then '完整' else '不完整' end as status length=8
  from companies as c
  left join have_cnt  as h
    on c.companyID = h.companyID
  order by status desc, companyID;
quit;

/* 7) 找重複的 company–year */
proc sql;
  create table duplicates_cy as
  select companyID, companyName, year_num, count(*) as n_rows
  from have
  group by companyID, year_num
  having calculated n_rows > 1
  order by companyID, year_num;
quit;

/* 8) 快速查看 */
title "每家公司完整性 (&y_min - &y_max)";
proc print data=company_completeness(obs=100) label noobs;
  label companyID='公司代碼' companyName='公司名稱'
        years_present='實際年數' years_expected='應有年數'
        years_missing='缺少年數' status='狀態';
run;

title "缺少的年（前 100 筆）";
proc print data=missing_list(obs=100) noobs; run;

title "重複的 company–year（前 100 筆）";
proc print data=duplicates_cy(obs=100) noobs; run;
title;

/* 9) 匯出到 Excel（同一檔案三個工作表） */
libname x xlsx "/home/u64061874/check_company_years_announce.xlsx";

data x.company_completeness; set company_completeness; run;
data x.missing_list;         set missing_list;         run;
data x.duplicates_cy;        set duplicates_cy;        run;

libname x clear;