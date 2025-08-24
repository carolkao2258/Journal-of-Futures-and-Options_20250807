/* 建議：先開啟訊息，跑穩後可改回 NONOTES 等 */
options notes stimer source syntaxcheck;

/* ========== 0) 檔案路徑與 fileref ========== */
filename fc   "/home/u64061874/foreign_company.xlsx";
filename p3_1 "/home/u64061874/patent_regression_data3_announce.xlsx";
filename out5 "/home/u64061874/patent_regression_data5_announce.xlsx";

/* ========== 1) 匯入資料 ========== */
proc import datafile=fc   out=foreign_company_raw             dbms=xlsx replace; getnames=yes; run;
proc import datafile=p3_1 out=patent_regression_data3_announce dbms=xlsx replace; getnames=yes; run;

/* ========== 2) 統一 companyID（不補零、不數值化，保留原樣） ========== */
%macro normalize_companyid(in=, out=, rename_dummy=, src_id=companyID);
data &out;
    set &in;
    length companyID_c $64;

    /* 先把來源欄位變成字串處理 */
    length _id_char $128;
    _id_char = strip(vvalue(&src_id));

    /* 全形數字→半形；移除空白（半形、全形、NBSP）；轉大寫 */
    _id_char = translate(_id_char, '0123456789', '０１２３４５６７８９');
    _id_char = compress(_id_char, ' '||'　'||byte(160));
    _id_char = upcase(_id_char);

    /* 直接保留清理後的樣子，不做數值轉換、不補零 */
    companyID_c = _id_char;

    drop &src_id _id_char;
    rename companyID_c = companyID
    %if %length(&rename_dummy) %then %do; &rename_dummy %end;
    ;
run;
%mend;

/* 對兩份資料都做清理（用短檔名避免 >32 bytes） */
%normalize_companyid(in=patent_regression_data3_announce,
                     out=p3_announce,
                     src_id=companyID);

%normalize_companyid(in=foreign_company_raw,
                     out=fc_norm,
                     rename_dummy= dummy=foreign_dummy,
                     src_id=companyID);

/* ========== 3) 合併（左併，以 p3_announce 為主；companyID 文字比對） ========== */
proc sql;
    create table p5_join_announce as
    select 
        a.*,
        b.foreign_company,
        b.foreign_dummy
    from p3_announce as a
    left join fc_norm as b
      on a.companyID = b.companyID
    ;
quit;

/* ========== 4) 覆寫特例（公司名/指定 ID） ========== */
data p5_edit_announce;
    set p5_join_announce;
    length _name $200;
    _name = upcase(compbl(strip(companyName)));

    /* 30142 → 港商匯豐詹金寶證券亞 → 是, 0 */
    if _name = upcase("港商匯豐詹金寶證券亞") then do;
        foreign_company = '是'; foreign_dummy = 0;
    end;

    /* 30869 → 新壽證券 → 否, 1 */
    if _name = upcase("新壽證券") then do;
        foreign_company = '否'; foreign_dummy = 1;
    end;

    /* 000154 → 是, 0 */
    if companyID = '000154' then do;
        foreign_company = '是'; foreign_dummy = 0;
    end;

    /* 000891 → 是, 0 */
    if companyID = '000891' then do;
        foreign_company = '是'; foreign_dummy = 0;
    end;

    drop _name;
run;

/* ========== 5) 刪除 foreign_company / foreign_dummy 其一缺失者 ========== */
data p5_fil_announce;
    set p5_edit_announce;
    if missing(foreign_company) or missing(foreign_dummy) then delete;
run;

/* ========== 6) 排序 + 固定欄位順序（保留 marginFac1、marginFac2） ========== */
proc sort data=p5_fil_announce out=p5_sort_announce; by year companyID; run;

data p5_final_announce; /* 這就是要匯出的工作資料集（短名） */
    retain 
        year companyID companyName
        invention_acc new_acc design_acc total_acc
        invention_sin new_sin design_sin total_sin
        ROE cost_to_income_ratio Zscore
        size FL current_ratio market_share ARTR PQ gGDP income_structure
        marginFac1 marginFac2
        foreign_company foreign_dummy;
    set p5_sort_announce;
run;

/* ========== 7) 匯出（保留原 companyID 字面樣式） ========== */
proc export data=p5_final_announce
    outfile=out5 dbms=xlsx replace;
    sheet="patent_regression_data5_announce";
    putnames=yes;
run;

/* 跑完可關閉訊息 */
options nonotes nostimer nosource nosyntaxcheck;