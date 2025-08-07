/* 匯入所有檔案 */
proc import out=list
     datafile="/home/u64061874/list.xlsx"
     dbms=xlsx replace;
     getnames=yes;
run;

proc import out=avg_PQ_final2_sorted
     datafile="/home/u64061874/avg_PQ_final2_sorted.xlsx"
     dbms=xlsx replace;
     getnames=yes;
run;

proc import out=financial_data
     datafile="/home/u64061874/financial_data.xlsx"
     dbms=xlsx replace;
     getnames=yes;
run;

proc import out=GDP_growth
     datafile="/home/u64061874/GDP_growth.xlsx"
     dbms=xlsx replace;
     getnames=yes;
run;

proc import out=patent_20250806
     datafile="/home/u64061874/patent_20250806.xlsx"
     dbms=xlsx replace;
     getnames=yes;
run;

/* ========= 1) 統一 PQ 表欄位名（apply_year→year、id→companyID） ========= */
data avg_PQ_final2_sorted2;
    set avg_PQ_final2_sorted;
    year = apply_year;
    companyID = id;
run;

/* ========= 2) 建立「落後一年」的 PQ 表（2004 用 2003 的 PQ） ========= */
proc sort data=avg_PQ_final2_sorted2 nodupkey;
  by companyID year;
run;

data avg_PQ_lag;
  set avg_PQ_final2_sorted2;
  year = year + 1;            /* 整體往後移一年 */
  rename avg_PQ = avg_PQ_lag;
run;

/* ========= 3) 合併（year、companyID 為鍵；僅用落後一年 PQ） ========= */
proc sql;
    create table merged_data as
    select 
        a.*,
        bl.avg_PQ_lag,
        c.ROE, c.Equity, c.TA, c.ROA, c.ARTR,
        d.GDP_growth,
        e.invention, e.new, e.design, e.total
    from list as a
    left join avg_PQ_lag as bl
        on a.year = bl.year and a.companyID = bl.companyID
    left join financial_data as c
        on a.year = c.year and a.companyID = c.companyID
    left join GDP_growth as d
        on a.year = d.year
    left join patent_20250806 as e
        on a.year = e.year and a.companyID = e.companyID
    ;
quit;

/* ========= 4) 允許缺失：avg_PQ_lag、invention、new、design、total 可缺 ========= */
data merged_data_nomiss;
    set merged_data;
    if ( cmiss(of _all_) 
         - cmiss(of avg_PQ_lag invention new design total) ) = 0;
run;

/* ========= 5) 專利欄缺失補 0：invention/new/design/total ========= */
data merged_data_filled;
    set merged_data_nomiss;
    array pat[4] invention new design total;
    do _i = 1 to dim(pat);
        if missing(pat[_i]) then pat[_i] = 0;
    end;
    drop _i;
run;

/* 取代第 6 步：只刪除五欄任一欄「只有 -」的列（保留負數） */
%let cols = ROE Equity TA ROA ARTR;

data merged_data_nohyphen;
    set merged_data_filled;

    length _var $32 _txt _clean $300;

    do _i = 1 to countw("&cols");
        _var = scan("&cols", _i);

        /* 取欄位的顯示值（不論原始型別），標準化 dash */
        _txt = vvaluex(_var);
        _txt = ktranslate(_txt, '-', '–—－');    /* EN/EM/全形 dash → 半形 '-' */
        _txt = kstrip(_txt);                     /* 去除前後各種空白（含全形） */

        /* 移除所有空白（含全形與不斷行）：若只剩 '-' 就刪 */
        _clean = kcompress(_txt, , 's');         /* 's' 移除所有空白類字元 */
        if _clean = '-' then delete;
    end;

    drop _i _var _txt _clean;
run;

proc sort data=merged_data_nohyphen out=md_preview;
  by year companyID;
run;

proc print data=md_preview (obs=20) noobs;
  var year companyID 
      avg_PQ_lag invention new design total
      ROE Equity TA ROA ARTR GDP_growth;
  title "merged_data_nohyphen - 前20筆檢視";
run;
title;

/* 1) 用 list 做公司名稱對照表（每個 companyID 只留一筆） */
proc sort data=list out=company_map(keep=companyID companyName) nodupkey;
  by companyID;
run;

/* 2) 若兩邊 companyID 型別不同，統一成字串再合併（穩健版） */
/* 2a) 對 merged_data_nohyphen 建字串鍵 */
data merged_with_key;
  set merged_data_nohyphen;
  length keyID $40;
  if vtypex('companyID')='N' then keyID = strip(put(companyID, best32.));
  else                           keyID = strip(companyID);
run;
/* 2b) 對 company_map 也建字串鍵 */
data company_map_key;
  set company_map;
  length keyID $40;
  if vtypex('companyID')='N' then keyID = strip(put(companyID, best32.));
  else                           keyID = strip(companyID);
  keep keyID companyName;
run;

/* 3) 合併中文名稱（list 為權威對照） */
proc sql;
  create table merged_with_cname as
  select m.*, c.companyName
  from merged_with_key as m
  left join company_map_key as c
    on m.keyID = c.keyID
  ;
quit;

/* 4) 快速檢視前 12 筆（可選） */
proc sql outobs=12;
  select year, companyID, companyName,
         avg_PQ_lag, invention, new, design, total,
         ROE, Equity, TA, ROA, ARTR, GDP_growth
  from merged_with_cname
  order by year, companyID;
quit;

/* 1) 先建立最終資料集，順便刪掉 keyID */
data patent_regression_data1;
  set merged_with_cname(drop=keyID);
run;

/* === 將 ROE / Equity / TA / ROA / ARTR 轉為數值（穩健清洗版） === */
%macro to_numeric(ds=, out=, vars=);
data &out;
  set &ds;
  %local i v n;
  %let n=%sysfunc(countw(&vars));
  %do i=1 %to &n;
    %let v=%scan(&vars,&i);
    /* 若本來就是數值，直接複製；否則清理後轉數值 */
    if vtypex("&v")='N' then &v._num = &v;
    else do;
      length __s $400;
      __s = vvaluex("&v");                 /* 取顯示值（不論原型別） */
      __s = ktranslate(__s, ' ', 'A0'x);   /* NBSP -> space */
      __s = ktranslate(__s, ',', '，');    /* 全形逗號 -> 半形逗號 */
      __s = kstrip(__s);                   /* 去前後各種空白 */
      if prxmatch('/^\(.*\)$/', __s) then  /* (1,234.5) -> -1234.5 */
         __s = cats('-', substr(__s,2,length(__s)-2));
      __s = compress(__s, ', ');           /* 去千分位逗號與空白 */
      if not missing(__s) and char(__s,length(__s))='%' then do;
         __s = substr(__s,1,length(__s)-1);
         &v._num = input(__s, best32.)/100;
      end;
      else &v._num = input(__s, best32.);
      drop __s;
    end;
    drop &v; rename &v._num=&v;
  %end;
run;
%mend;

/* 1) 轉數值（輸入是你已經整理好的 patent_regression_data1） */
%to_numeric(ds=patent_regression_data1,
           out=patent_regression_data1_num,
           vars=ROE Equity TA ROA ARTR);
/* 若也要把 GDP_growth 轉成小數，直接把它加到 vars= 後面即可 */

/* 2) 依 year、companyID 排序 */
proc sort data=patent_regression_data1_num
          out=patent_regression_data1_srt;
  by year companyID;
run;

/* 3) 匯出排序後版本到 Excel */
proc export data=patent_regression_data1_srt
    outfile="/home/u64061874/patent_regression_data1.xlsx"
    dbms=xlsx replace;
    sheet="patent_regression_data1";
    putnames=yes;
run;
