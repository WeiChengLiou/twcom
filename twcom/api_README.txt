Taiwan Company's Network Rest API

1. company network from company id

url/com?id=
id: company id

2. boards network from company id

url/boss?id=
id: company id

3. company network from boss name

url/com?boss=
boss: boss name

4. company network from boards of the same company

url/com?comboss=
comboss: company id

5. company network from address of the company

url/com?comaddr=
comaddr: company id

6. query boss with similar name (in SQL: "name like '%boss%'")

url/query?boss=
boss: boss name

7. query company name and id from company name (in SQL: "name like '%name%'")

url/query?name=
name: company name

