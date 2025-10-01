Můj myšlenkový pochod byl nejprve rozdělit si úkol na malé a snadno splnitelné části. 
Flow teoretického řešení (viz „pseudodiagram“). Diagram jsem tvořil jen pro sebe.  

Program byl zpočátku vytvořen bez jakéhokoliv multithreadingu či kontrol – čistě jen 
pro ověření, zda samotné flow dává smysl. Následně byly přidány jednotlivé kontroly 
a jako poslední i multithreading. Koncepce multithreadingu je popsána přímo v kódu.  

Zádrhely:  
    Asi největším problémem byla nedostatečná znalost technologií. Proto vznikl dokument 
    o délce přibližně 45 000 znaků, který shrnuje látku co nejvíce je možné, ale i tak 
    zdaleka neobsahuje vše.  

    Pokud mám být konkrétní, tak jedním velkým zádrhelem bylo připojení ClickHouse ke Kafce. 
    Nakonec jsem zjistil, že Kafka server měl v konfiguraci vypnuté jakékoliv odchozí 
    připojení – ať už IPv4 či IPv6. Bohužel ani Kafka error log v tomto případě nebyl 
    příliš užitečný, protože jsem narážel na nekonzistentní chování. Po několika opravách 
    jsem konečně dostal error, který mě navedl k tomu, co přesně je třeba změnit v konfiguraci 
    serveru.  

    Dalším zádrhelem byla samotná transformace. Po prvním návrhu a implementaci kódu jsem zjistil, 
    jak neoptimální řešení to bylo, protože porušovalo princip imutability. Serializoval jsem 
    vlastní struktury a tím vznikala zbytečná operace – bylo by nutné ručně opětovně balit 
    struktury do capnp formátu.  

    Nakonec zde byla ještě Grafana. Myslím, že jsem splnil vše až na část, která požadovala 
    „Retention of aggregated data“. Na to jsem nedokázal přijít. Řešení tam sice je, ale pochybuji, 
    že je správné.  

Řešení vzniklo na základě několika iterací skrze jednotlivé zádrhely, na které jsem narazil, 
společně s teoretickými znalostmi, které jsem postupně získával.  

Musím se nakonec přiznat, že jsem měl celý svůj myšlenkový pochod dokumentovat už od začátku. 
Pokud máte zájem, mohu poslat kopii všech poznámek, které při práci vznikly.