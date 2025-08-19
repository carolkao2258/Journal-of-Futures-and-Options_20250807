/* ========= 1) 匯入兩份檔案 ========= */
proc import datafile="/home/u64061874/merge.xlsx"
    out=merge_raw dbms=xlsx replace;
    getnames=yes;
run;

proc import datafile="/home/u64061874/patent_regression_data3_announce.xlsx"
    out=patent_raw dbms=xlsx replace;
    getnames=yes;
run;

/* ========= 2) 正規化公司名稱欄位，用於比對 ========= */
data merge_n;
    set merge_raw;
    length name_key $200;
    name_key = upcase(compbl(strip(securuties)));
run;

data patent_n;
    set patent_raw;
    length name_key $200 companyID_c $32;

    name_key = upcase(compbl(strip(companyName)));

    if vtype(companyID)='N' then companyID_c = put(companyID, z6.);
    else do;
        companyID_c = strip(companyID);
        companyID_c = translate(companyID_c, '0123456789', '０１２３４５６７８９');
        companyID_c = compress(companyID_c, ' '||'　'||byte(160));
        companyID_c = upcase(companyID_c);
    end;

    drop companyID;
    rename companyID_c = companyID;
run;

/* ========= 3) 以公司名稱左併（merge 為主）帶入 companyID ========= */
proc sql;
    create table merged_out as
    select 
        b.companyID length=32,
        a.securuties,
        a.foreign_company,
        a.dummy
    from merge_n as a
    left join patent_n as b
      on a.name_key = b.name_key
    ;
quit;

/* ========= 4) 去除重複公司（根據 securuties） ========= */
proc sort data=merged_out nodupkey out=merged_unique;
    by securuties;
run;

/* ========= 5) 按 companyID 排序 ========= */
proc sort data=merged_unique out=merged_sorted;
    by companyID;
run;

/* ========= 6) 匯出（對應非 patent_regression_dataX，不加 _announce） ========= */
proc export data=merged_sorted
    outfile="/home/u64061874/foreign_company.xlsx"
    dbms=xlsx replace;
    sheet="merged_sorted";
    putnames=yes;
run;

/* ========= 1) 匯入資料 ========= */
proc import datafile="/home/u64061874/foreign_company.xlsx"
    out=foreign_company_raw dbms=xlsx replace;
    getnames=yes;
run;

proc import datafile="/home/u64061874/patent_regression_data3_announce.xlsx"
    out=patent_regression_data3_announce dbms=xlsx replace;
    getnames=yes;
run;

/* ========= 2) 統一 companyID 型態（保留前導 0） ========= */
/* foreign_company */
data foreign_company;
    set foreign_company_raw;
    length companyID_c $32;
    if vtype(companyID)='N' then companyID_c = put(companyID, z6.);
    else do;
        companyID_c = strip(companyID);
        companyID_c = translate(companyID_c, '0123456789', '０１２３４５６７８９');
        companyID_c = compress(companyID_c, ' '||'　'||byte(160));
        companyID_c = upcase(companyID_c);
    end;
    drop companyID;
    rename companyID_c = companyID dummy = foreign_dummy; /* 這裡改名 */
run;

/* patent_regression_data3_announce → 正規化 companyID */
data patent3_announce;
    set patent_regression_data3_announce;
    length companyID_c $32;
    if vtype(companyID)='N' then companyID_c = put(companyID, z6.);
    else do;
        companyID_c = strip(companyID);
        companyID_c = translate(companyID_c, '0123456789', '０１２３４５６７８９');
        companyID_c = compress(companyID_c, ' '||'　'||byte(160));
        companyID_c = upcase(companyID_c);
    end;
    drop companyID;
    rename companyID_c = companyID;
run;

/* ========= 3) 合併（左併，以 patent3_announce 為主） ========= */
proc sql;
    create table patent_regression_data5_announce as
    select 
        a.*,
        b.foreign_company,
        b.foreign_dummy
    from patent3_announce as a
    left join foreign_company as b
      on a.companyID = b.companyID
    ;
quit;

/* ========= 4) 調整欄位順序 ========= */
data patent_regression_data5_announce;
    retain 
        year companyID companyName
        invention_acc new_acc design_acc total_acc
        invention_sin new_sin design_sin total_sin
        ROE cost_to_income_ratio Zscore
        size FL current_ratio market_share ARTR PQ gGDP income_structure marginFac
        foreign_company foreign_dummy;
    set patent_regression_data5_announce;
run;

/* ========= 5) 按 year、companyID 排序 ========= */
proc sort data=patent_regression_data5_announce;
    by year companyID;
run;

/* === 覆寫：直接用公司名稱比對（避免 ID 格式差異）=== */
data patent_regression_data5_announce;
    set patent_regression_data5_announce;

    length _name $200;
    _name = upcase(compbl(strip(companyName)));  /* 正規化公司名 */

    /* 30142 → 港商匯豐詹金寶證券亞 → 是, 0 */
    if _name = upcase("港商匯豐詹金寶證券亞") then do;
        foreign_company = '是';
        foreign_dummy = 0;
    end;

    /* 30869 → 新壽證券 → 否, 1 */
    if _name = upcase("新壽證券") then do;
        foreign_company = '否';
        foreign_dummy = 1;
    end;

    /* 000154 → 是, 0 */
    if companyID = '000154' then do;
        foreign_company = '是';
        foreign_dummy = 0;
    end;

    /* 000891 → 是, 0 */
    if companyID = '000891' then do;
        foreign_company = '是';
        foreign_dummy = 0;
    end;

    drop _name;
run;

/* 刪除缺失值 */
data patent_regression_data5_announce;
    set patent_regression_data5_announce;
    if missing(foreign_company) or missing(foreign_dummy) then delete;
run;

/* 固定欄位順序 */
data patent_regression_data5_announce;
    retain 
        year companyID companyName
        invention_acc new_acc design_acc total_acc
        invention_sin new_sin design_sin total_sin
        ROE cost_to_income_ratio Zscore
        size FL current_ratio market_share ARTR PQ gGDP income_structure marginFac
        foreign_company foreign_dummy;
    set patent_regression_data5_announce;
run;

/* 排序 */
proc sort data=patent_regression_data5_announce;
    by year companyID;
run;

/* 匯出結果（patent_regression_dataX 類別 → 帶 _announce） */
proc export data=patent_regression_data5_announce
    outfile="/home/u64061874/patent_regression_data5_announce.xlsx"
    dbms=xlsx replace;
    sheet="patent_regression_data5_announce";
    putnames=yes;
run;