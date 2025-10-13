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

K dodělání kódu došlo kvůli nespokojenosti. Obecně jsem se ten kód snažil psát více expresivně 
kvůli sobě, ale se vší upřímností to na produkci nikoho nezajímá. 
    Změny: Optimizaliace tvoření protokolů, abych využil smart pointer a nechal config objekt
           smazat jakmile byl využit při tvorbě objektů (producer, consumer a topic). 

           Přidání json confingu, který mě zbavil toho řešení s Roles a sjednocení na jednu config
           funkci. 

           Smazání zbytků nepoužitého kódu.

Tento týden jsem provedl refaktoring, který ještě lépe zorganizoval tvorbu protokolů pomocí 
návrhového vzoru factor. Byla pro to založena separátní větev. Spoléhám se pouze na to, že znám 
názvy svého consumeru, produceru nebo topicu v konfiguraci. Při vytváření pak stačí zadat jejich 
jméno a systém je automaticky vytvoří i nakonfiguruje v jednom kroku.

Retrospektivně mi dochází, že jsem si dostatečně nepředstavil, jak aplikace běží na origin serveru a 
zpracovává veškerá data jednotlivých PoPů, a tím pádem jsem neaplikoval odpovídající škálování. 
Nastavení počtu konzumentů v Kafka Engine už samo o sobě není takový problém. Výsledně by však bylo 
nutné doladit multithreadovou logiku, protože ConfigBasedFactory mi umožňuje vytvořit libovolný 
počet konzumentů a producerů.
Řešení by pravděpodobně spočívalo v tom, že data přicházející na http_log by bylo nutné partitionovat, 
abych zachoval princip one consumer per partition. Toto teoretické rozdělení musí mít svůj partition 
key jako ideální kandidát se jeví resourceId. Takto by mohlo běžet několik instancí stejné 
stream-processing aplikace. Důvodem použití klíče je to, abych data logicky rozdělil a nespoléhal 
pouze na round-robin. Pořadí zpráv bude zajištěno pomocí offsetu, který si jednotliví konzumenti ve 
stejné skupině koordinují s Kafkou, jež uchovává offsety všech konzumentů.

Škálovatelná cesta, jak postupovat po transformaci dat: 
    První řešení by bylo ekvivalentní počet partitionů v topiku pro transformované logy.
    To znamená například, že pokud jsem rozdělil data tak, že vše s resourceId = 345 spadá do 
    partition 1 v topiku http_log, pak tato data půjdou i do partition 1 v topiku 
    http_log_transformed. V takovém případě bude nutné ručně nastavit partition pro Kafka Engine 
    tabulku v ClickHouse. Takových tabulek může být mnoho, proto je vhodné mít více klíčů pro jeden 
    partition – například rozdělení podle lokace. Pokud například vím, že dané resourceId odpovídá 
    regionu EU-WEST, mohou tato data spadat společně do partition 1, ale záleží na use case. 