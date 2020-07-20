## generate_random_data

### compile

```sql
> \i generate_random_data.sql
```

### execute

example

```sql
> call generate_random_data(1000000, 1000, '2020-01-01', '2020-12-31', TRUE);
```

#### araguments

|pos  |description |default |
|:---:|:-----------|:-------|
| 1   | 生成するレコード件数を指定 |  |
| 2   | コミットする単位 | 1000 |
| 3   | ランダムに生成する日付の範囲(from) | '1901-01-01' |
| 4   | ランダムに生成する日付の範囲(to) | '2099-12-31' |
| 5   | 実行時にrandom_tblをtruncateするか | false |

#### measure execution time

```sql
> \timing
> call generate_random_date(100000);
```
