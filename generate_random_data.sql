SET plpgsql.extra_warnings TO 'all';
SET plpgsql.extra_errors TO 'all';

DROP FUNCTION IF EXISTS gen_random_integer();

/**
 * 0から2147483647までの範囲でランダムな数値を返す
 */
CREATE OR REPLACE FUNCTION gen_random_integer() RETURNS INTEGER
LANGUAGE plpgsql
AS $$
BEGIN
  RETURN (RANDOM() * 2147483647)::INTEGER;
END
$$;

DROP FUNCTION IF EXISTS gen_random_bigint();

/**
 * 0から9223372036854775807までの範囲でランダムな数値を返す
 */
CREATE OR REPLACE FUNCTION gen_random_bigint() RETURNS BIGINT
LANGUAGE plpgsql
AS $$
BEGIN
  RETURN (RANDOM() * 9223372036854775807)::BIGINT;
END
$$;

DROP FUNCTION IF EXISTS gen_random_varchar();

/**
 * 200文字から1000文字のランダムな文字列を返す
 */
CREATE OR REPLACE FUNCTION gen_random_varchar() RETURNS VARCHAR
LANGUAGE plpgsql
AS $$
DECLARE
  v_rnd INTEGER := 0;
  v_tmp VARCHAR := '';
  v_str VARCHAR := '';
BEGIN
  --1から5の乱数
  v_rnd := (RANDOM() * 4)::INTEGER + 1;

  --1ループあたり200文字のランダムな文字列を生成する
  --ループ回数はランダム、1ループで200文字、5ループで1000文字
  FOR i IN 1..v_rnd LOOP
    --code point 12353(ぁ) から 12435(ん)までのランダムな文字を100文字連結する
    SELECT STRING_AGG(CHR(12353 + (RANDOM() * 82)::INTEGER), '') INTO v_tmp FROM GENERATE_SERIES(1, 100);
    v_str := v_str || v_tmp;
    --code point 12449(ァ) から 12533(ヵ)までのランダムな文字を100文字連結する
    SELECT STRING_AGG(CHR(12449 + (RANDOM() * 84)::INTEGER), '') INTO v_tmp FROM GENERATE_SERIES(1, 100);
    v_str := v_str || v_tmp;
  END LOOP;

  RETURN v_str;
END
$$;

DROP FUNCTION IF EXISTS gen_random_date(DATE, DATE);

/**
 * パラメータv_date_from から v_date_toの範囲でランダムな日付を返す
 */
CREATE OR REPLACE FUNCTION gen_random_date(v_date_from DATE, v_date_to DATE) RETURNS DATE
LANGUAGE plpgsql
AS $$
DECLARE
  v_rnd INTEGER := 0;
  v_numOfDays INTEGER := v_date_to - v_date_from;
BEGIN
  v_rnd := (RANDOM() * v_numOfDays)::INTEGER;
  RETURN v_date_from + v_rnd;
END
$$;

DROP FUNCTION IF EXISTS gen_random_boolean();

/**
 * ランダムにtrue/falseを返す
 */
CREATE OR REPLACE FUNCTION gen_random_boolean() RETURNS BOOLEAN
LANGUAGE plpgsql
AS $$
DECLARE
  v_rnd INTEGER := 0;
BEGIN
  v_rnd := (RANDOM() * 1)::INTEGER;
  IF v_rnd = 0 THEN
    RETURN FALSE;
  ELSE
    RETURN TRUE;
  END IF;
END
$$;

DROP PROCEDURE IF EXISTS generate_random_data(INTEGER, INTEGER, DATE, DATE, BOOLEAN);

/**
 * ランダムなデータを生成するメイン処理
 */
CREATE OR REPLACE PROCEDURE generate_random_data(
  v_generate_num INTEGER,                -- 生成件数
  v_commit_num INTEGER DEFAULT 1000,     -- コミットする件数
  v_date_from DATE DEFAULT '1901-01-01', -- 生成する日付の範囲(開始)
  v_date_to DATE DEFAULT '2099-12-31',   -- 生成する日付の範囲(終了)
  v_truncate BOOLEAN DEFAULT FALSE       -- truncateするか
)
LANGUAGE plpgsql
AS $$
DECLARE
  v_seed DOUBLE PRECISION := TO_CHAR(CURRENT_TIMESTAMP, 'US')::INTEGER * 0.000001;
  v_row random_tbl%ROWTYPE;
BEGIN
  RAISE NOTICE 'calling ''generate_random_data'' at %.', now();
  RAISE NOTICE 'args generate_num:(%) commit_num:(%) date_from:(%) date_to:(%) truncate:(%)', v_generate_num, v_commit_num, v_date_from, v_date_to, v_truncate;

  IF v_date_from > v_date_to THEN
    RAISE EXCEPTION 'invalid date range from:(%) to:(%)', v_date_from, v_date_to USING HINT = 'check v_date_from or v_date_to parameter';
  END IF;

  IF v_truncate = TRUE THEN
    RAISE NOTICE 'truncate table random_tbl';
    TRUNCATE TABLE random_tbl;
  END IF;

  --RANDOM()のseedを設定
  PERFORM SETSEED(v_seed);

  FOR i IN 1..v_generate_num LOOP
    v_row.fld_int1 := gen_random_integer();
    v_row.fld_var2 := gen_random_varchar();
    v_row.fld_boo3 := gen_random_boolean();
    v_row.fld_dat4 := gen_random_date(v_date_from, v_date_to);
    v_row.fld_big5 := gen_random_bigint();
    -- RAISE NOTICE 'loop i : (%) % % % % %', i, v_row.fld_int1, v_row.fld_var2, v_row.fld_boo3, v_row.fld_dat4, v_row.fld_big5;
    INSERT INTO random_tbl (fld_int1, fld_var2, fld_boo3, fld_dat4, fld_big5) VALUES (v_row.fld_int1, v_row.fld_var2, v_row.fld_boo3, v_row.fld_dat4, v_row.fld_big5);
    IF i % v_commit_num = 0 THEN
      RAISE NOTICE 'commit (%)', i;
      COMMIT;
    END IF;
  END LOOP;

END
$$;


/*
call generate_random_data(1000000, 1000, '1901-01-01', '1910-12-31', TRUE);
call generate_random_data(1000000, 1000, '1911-01-01', '1920-12-31');
call generate_random_data(1000000, 1000, '1921-01-01', '1930-12-31');
call generate_random_data(1000000, 1000, '1931-01-01', '1940-12-31');
call generate_random_data(1000000, 1000, '1941-01-01', '1950-12-31');
call generate_random_data(1000000, 1000, '1951-01-01', '1960-12-31');
call generate_random_data(1000000, 1000, '1961-01-01', '1970-12-31');
call generate_random_data(1000000, 1000, '1971-01-01', '1980-12-31');
call generate_random_data(1000000, 1000, '1981-01-01', '1990-12-31');
call generate_random_data(1000000, 1000, '1991-01-01', '2000-12-31');

call generate_random_data(1000000, 1000, '2001-01-01', '2010-12-31');
call generate_random_data(1000000, 1000, '2011-01-01', '2020-12-31');
call generate_random_data(1000000, 1000, '2021-01-01', '2030-12-31');
call generate_random_data(1000000, 1000, '2031-01-01', '2040-12-31');
call generate_random_data(1000000, 1000, '2041-01-01', '2050-12-31');
call generate_random_data(1000000, 1000, '2051-01-01', '2060-12-31');
call generate_random_data(1000000, 1000, '2061-01-01', '2070-12-31');
call generate_random_data(1000000, 1000, '2071-01-01', '2080-12-31');
call generate_random_data(1000000, 1000, '2081-01-01', '2090-12-31');
call generate_random_data(1000000, 1000, '2091-01-01', '2099-12-31');
*/
