import requests
from bs4 import BeautifulSoup
import datetime
import os
import time
import re
import sqlite3
import gzip
import concurrent.futures
import json
import xml.etree.ElementTree as ET

# --- KONFIGURACJA GŁÓWNA ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_FILE = os.path.join(BASE_DIR, "epg_onet.xml.gz")
DB_FILE = os.path.join(BASE_DIR, "epg_baza.db")

# Dni pobierane z Onetu
DNI_DO_POBRANIA = [0, 1, 12]
# Budowa CatchUp (ile dni wstecz trzymamy w bazie)
DNI_CATCHUP = 7
# Ilość jednocześnie pobieranych kanałów
MAX_WATKOW = 10
# Głębokie skanowanie szczegółów
DEEP_SCAN = True

# Link do zewnętrznego EPG dla brakujących stacji
EXTERNAL_EPG_URL = "https://iptv.otopay.io/guide.xml"

# --- LISTY KANAŁÓW ---
# Wklej kanały dla Onetu: "Nazwa": ("slug-onetu", "ID.pl")
CHANNELS_ONET = {
    "TVP HD": ("tvp-hd-101", "TVPHD.pl"),
    "TVP 1 HD": ("tvp-1-hd-380", "TVP1HD.pl"),
    "TVP 2 HD": ("tvp-2-hd-145", "TVP2HD.pl"),
    "TVP 3 HD": ("tvp-3-172", "TVP3HD.pl"),
    "TVP Kobieta": ("tvp-kobieta", "TVPKobieta.pl"),
    "TVP 3": ("tvp-3-172", "TVP3.pl"),
    "Polonia 1": ("polonia-1-328", "Polonia1.pl"),
    "Polonia 1 HD": ("polonia-1-328", "Polonia1HD.pl"),
    "TVP Nauka HD": ("tvp-nauka", "TVPNaukaHD.pl"),
    "TVP Polonia": ("tvp-polonia-325", "TVPPolonia.pl"),
    "TVN HD": ("tvn-hd-98", "TVNHD.pl"),
    "TVP Rozrywka HD": ("tvp-rozrywka-159", "TVPRozrywkaHD.pl"),
    "TVN 7 HD": ("tvn-7-hd-142", "TVN7HD.pl"),
    "TVP Rozrywka": ("tvp-rozrywka-159", "TVPRozrywka.pl"),
    "Polsat HD": ("polsat-hd-35", "PolsatHD.pl"),
    "TVP Kultura HD": ("tvp-kultura-hd-680", "TVPKulturaHD.pl"),
    "Polsat 2 HD": ("polsat-2-hd-218", "Polsat2HD.pl"),
    "Super Polsat HD": ("super-polsat-hd-560", "SuperPolsatHD.pl"),
    "TV Puls HD": ("tv-puls-hd-197", "TVPulsHD.pl"),
    "TV4 HD": ("tv-4-hd-222", "TV4HD.pl"),
    "TV6 HD": ("tv-6-hd-561", "TV6HD.pl"),
    "Tele 5 HD": ("tele-5-niem-448", "Tele5HD.pl"),
    "Tele 5": ("tele-5-niem-448", "Tele5.pl"),
    "Metro HD": ("metro-hd-536", "MetroHD.pl"),
    "WP HD": ("wp-hd-533", "WPHD.pl"),
    "WP": ("wp-hd-533", "WP.pl"),
    "Zoom TV HD": ("zoom-tv-hd-527", "ZoomTVHD.pl"),
    "Zoom TV": ("zoom-tv-hd-527", "ZoomTV.pl"),
    "Nowa TV": ("nowa-tv-hd-529", "NowaTV.pl"),
    "Home TV HD": ("tvr-hd-170", "HomeTVHD.pl"),
    "Home TV": ("tvr-hd-170", "HomeTV.pl"),
    "TVP Info": ("tvp-info-hd-525", "TVPInfo.pl"),
    "TVN24": ("tvn-24-hd-158", "TVN24.pl"),
    "TVN24 BiS": ("tvn-24-biznes-i-swiat-hd-537", "TVN24BiS.pl"),
    "TVP World HD": ("tvp-world", "TVPWorldHD.pl"),
    "Polsat News": ("polsat-news-hd-229", "PolsatNews.pl"),
    "TVP World": ("tvp-world", "TVPWorld.pl"),
    "Polsat News 2": ("polsat-news-2-hd-671", "PolsatNews2.pl"),
    "Wydarzenia 24": ("superstacja-hd-550", "Wydarzenia24.pl"),
    "Polsat News Polityka": ("polsat-news-polityka", "PolsatNewsPolityka.pl"),
    "wPolsce24": ("wpolsce-pl-hd-637", "wPolsce24.pl"),
    "wPolsce24 HD": ("wpolsce-pl-hd-637", "wPolsce24HD.pl"),
    "Biznes24": ("biznes24-hd-686", "Biznes24.pl"),
    "News 24": ("news24", "News24.pl"),
    "TVP Seriale": ("tvp-seriale-130", "TVPSeriale.pl"),
    "Polsat Seriale HD": ("polsat-romans-173", "PolsatSerialeHD.pl"),
    "Canal+ 360 HD": ("canal-family-hd-297", "CanalPlus360HD.pl"),
    "Alfa TVP HD": ("alfa-tvp", "AlfaTVPHD.pl"),
    "TVN Turbo HD": ("tvn-turbo-hd-143", "TVNTurboHD.pl"),
    "Motowizja HD": ("motowizja-hd-194", "MotowizjaHD.pl"),
    "Polsat Games HD": ("polsat-games-hd-670", "PolsatGamesHD.pl"),
    "TBN Polska": ("tbn-polska-hd-621", "TBNPolska.pl"),
    "Gametoon HD": ("gametoon-hd-602", "GametoonHD.pl"),
    "Comedy Central HD": ("comedy-central-hd-60", "ComedyCentralHD.pl"),
    "FX Comedy HD": ("fox-comedy-hd-405", "FXComedyHD.pl"),
    "Polsat Comedy Central Extra": ("comedy-central-family-hd-612", "PolsatComedyCentralExtra.pl"),
    "TVC HD": ("ntl-radomsko-184", "TVCHD.pl"),
    "TVC": ("ntl-radomsko-184", "TVC.pl"),
    "AXN HD": ("axn-hd-286", "AXNHD.pl"),
    "AXN": ("axn-hd-286", "AXN.pl"),
    "AXN Spin HD": ("axn-spin-hd-292", "AXNSpinHD.pl"),
    "AXN Black": ("axn-black-271", "AXNBlack.pl"),
    "AXN White": ("axn-white-272", "AXNWhite.pl"),
    "TTV HD": ("ttv-33", "TTVHD.pl"),
    "TLC HD": ("tlc-hd-163", "TLCHD.pl"),
    "BBC First PL": ("bbc-hd-261", "BBCFirstPL.pl"),
    "Epic Drama HD": ("epic-drama-hd-603", "EpicDramaHD.pl"),
    "Romance TV": ("romance-tv-hd-139", "RomanceTV.pl"),
    "HGTV HD": ("hgtv-hd-558", "HGTVHD.pl"),
    "Canal+ Domo HD": ("domo-hd-437", "CanalPlusDomoHD.pl"),
    "Canal+ Kuchnia HD": ("kuchnia-hd-434", "CanalPlusKuchniaHD.pl"),
    "Food Network HD": ("polsat-food-network-hd-209", "FoodNetworkHD.pl"),
    "TVN Style HD": ("tvn-style-hd-141", "TVNStyleHD.pl"),
    "Polsat Rodzina": ("polsat-rodzina-hd-672", "PolsatRodzina.pl"),
    "Polsat Café HD": ("polsat-caf-hd-219", "PolsatCaféHD.pl"),
    "Polsat Play HD": ("polsat-play-hd-22", "PolsatPlayHD.pl"),
    "Da Vinci HD": ("da-vinci-hd-614", "DaVinciHD.pl"),
    "Da Vinci": ("da-vinci-hd-614", "DaVinci.pl"),
    "English Club TV": ("english-club-tv-hd-181", "EnglishClubTV.pl"),
    "Water Planet": ("water-planet-hd-156", "WaterPlanet.pl"),
    "Adventure HD": ("adventure-hd-305", "AdventureHD.pl"),
    "Fast FunBox": ("fast-funbox-hd-104", "FastFunBox.pl"),
    "Active Family": ("active-family-hd-301", "ActiveFamily.pl"),
    "E! Entertainment": ("e-entertainment-hd-169", "E!Entertainment.pl"),
    "FashionBox HD": ("fashionbox-hd-171", "FashionBoxHD.pl"),
    "StudioMed TV HD": ("studiomed-tv-688", "StudioMedTVHD.pl"),
    "TV Okazje": ("tv-okazje-hd-633", "TVOkazje.pl"),
    "MyZen 4K PL": ("myzen-4k", "MyZen4KPL.pl"),
    "Antena HD": ("antena", "AntenaHD.pl"),
    "Antena": ("antena", "Antena.pl"),
    "13 Ulica": ("13-ulica-hd-509", "13Ulica.pl"),
    "AMC PL": ("mgm-hd-68", "AMCPL.pl"),
    "Warner TV": ("tnt-hd-220", "WarnerTV.pl"),
    "Ale Kino+": ("ale-kino-hd-262", "AleKinoPlus.pl"),
    "Canal+ Premium": ("canal-hd-288", "CanalPlusPremium.pl"),
    "Canal+ 1 HD": ("canal-1-hd-299", "CanalPlus1HD.pl"),
    "Canal+ Film": ("canal-film-hd-278", "CanalPlusFilm.pl"),
    "HBO": ("hbo-hd-26", "HBO.pl"),
    "HBO 2": ("hbo2-hd-27", "HBO2.pl"),
    "HBO 3": ("hbo-3-hd-28", "HBO3.pl"),
    "Cinemax": ("cinemax-hd-57", "Cinemax.pl"),
    "Cinemax 2": ("cinemax2-hd-56", "Cinemax2.pl"),
    "Polsat Film HD": ("polsat-film-hd-162", "PolsatFilmHD.pl"),
    "TVN Fabuła HD": ("tvn-fabula-hd-37", "TVNFabułaHD.pl"),
    "FX HD": ("fox-hd-128", "FXHD.pl"),
    "Filmax HD": ("filmax", "FilmaxHD.pl"),
    "ViDoc TV HD": ("ctv9", "ViDocTVHD.pl"),
    "Stopklatka": ("stopklatka-hd-186", "Stopklatka.pl"),
    "Kino Polska HD": ("kino-polska-hd-658", "KinoPolskaHD.pl"),
    "Kino TV HD": ("kino-tv-hd-663", "KinoTVHD.pl"),
    "FilmBox Premium": ("filmbox-premium-85", "FilmBoxPremium.pl"),
    "FilmBox Extra HD": ("filmbox-extra-hd-86", "FilmBoxExtraHD.pl"),
    "FilmBox Action": ("filmbox-action-451", "FilmBoxAction.pl"),
    "FilmBox Family": ("filmbox-family-103", "FilmBoxFamily.pl"),
    "FilmBox ArtHouse": ("filmbox-arthouse-hd-190", "FilmBoxArtHouse.pl"),
    "Sundance TV HD": ("sundance-channel-hd-392", "SundanceTVHD.pl"),
    "SciFi": ("sci-fi-hd-628", "SciFi.pl"),
    "Bollywood HD": ("bollywood-hd-530", "BollywoodHD.pl"),
    "Novelas+ HD": ("novelas", "NovelasPlusHD.pl"),
    "Novela TV": ("novela-tv-hd-155", "NovelaTV.pl"),
    "BBC Earth": ("bbc-earth-hd-263", "BBCEarth.pl"),
    "BBC Brit": ("bbc-brit-hd-306", "BBCBrit.pl"),
    "Planete+": ("planete-hd-432", "PlanetePlus.pl"),
    "Canal+ Dokument": ("canal-discovery-hd-308", "CanalPlusDokument.pl"),
    "BBC Lifestyle HD": ("bbc-lifestyle-hd-542", "BBCLifestyleHD.pl"),
    "History": ("history-hd-niem-458", "History.pl"),
    "History 2": ("h2-hd-205", "History2.pl"),
    "Discovery Historia": ("discovery-historia-54", "DiscoveryHistoria.pl"),
    "Discovery": ("discovery-hd-niem-450", "Discovery.pl"),
    "Discovery Science": ("discovery-science-hd-52", "DiscoveryScience.pl"),
    "Discovery Life": ("discovery-life-hd-547", "DiscoveryLife.pl"),
    "DTX": ("discovery-turbo-xtra-hd-189", "DTX.pl"),
    "Nat Geo People": ("nat-geo-people-hd-211", "NatGeoPeople.pl"),
    "Polsat Viasat History HD": ("polsat-viasat-history-hd-71", "PolsatViasatHistoryHD.pl"),
    "Animal Planet HD": ("animal-planet-niem-264", "AnimalPlanetHD.pl"),
    "Polsat Viasat Explore HD": ("polsat-viasat-explore-hd-82", "PolsatViasatExploreHD.pl"),
    "Polsat Viasat Nature HD": ("polsat-viasat-nature-413", "PolsatViasatNatureHD.pl"),
    "TVP Historia": ("tvp-historia-74", "TVPHistoria.pl"),
    "TVP Historia HD": ("tvp-historia-74", "TVPHistoriaHD.pl"),
    "TVP Dokument HD": ("tvp-dokument", "TVPDokumentHD.pl"),
    "Polsat Doku HD": ("polsat-doku-hd-551", "PolsatDokuHD.pl"),
    "Fokus TV": ("fokus-tv-hd-47", "FokusTV.pl"),
    "Travel Channel HD": ("travel-channel-hd-152", "TravelChannelHD.pl"),
    "Docubox Polska": ("docubox-hd-175", "DocuboxPolska.pl"),
    "Eurosport 1 HD": ("eurosport-niem-366", "Eurosport1HD.pl"),
    "Eurosport 2 HD": ("eurosport-2-hd-120", "Eurosport2HD.pl"),
    "Eleven Sports 1 4K": ("eleven-sports-1-4k-667", "ElevenSports14K.pl"),
    "Eleven Sports 1 HD": ("eleven-hd-227", "ElevenSports1HD.pl"),
    "Eleven Sports 2 HD": ("eleven-hd-sports-228", "ElevenSports2HD.pl"),
    "Eleven Sports 3 HD": ("eleven-extra-hd-534", "ElevenSports3HD.pl"),
    "Eleven Sports 4 HD": ("eleven-sports-4-hd-611", "ElevenSports4HD.pl"),
    "Polsat Sport 1": ("polsat-sport-hd-96", "PolsatSport1.pl"),
    "Polsat Sport 2": ("polsat-sport-extra-hd-144", "PolsatSport2.pl"),
    "Polsat Sport 3": ("polsat-sport-news-hd-543", "PolsatSport3.pl"),
    "Polsat Sport Fight HD": ("polsat-sport-fight-521", "PolsatSportFightHD.pl"),
    "Polsat Sport Extra 1": ("polsat-sport-premium-3-645", "PolsatSportExtra1.pl"),
    "Polsat Sport Extra 2": ("polsat-sport-premium-4-646", "PolsatSportExtra2.pl"),
    "Polsat Sport Extra 3": ("polsat-sport-premium-5-642", "PolsatSportExtra3.pl"),
    "Polsat Sport Extra 4": ("polsat-sport-premium-6-641", "PolsatSportExtra4.pl"),
    "TVP Sport HD": ("tvp-sport-hd-39", "TVPSportHD.pl"),
    "Canal+ Sport HD": ("canal-sport-hd-12", "CanalPlusSportHD.pl"),
    "Canal+ Sport 2 HD": ("canal-sport-2-hd-13", "CanalPlusSport2HD.pl"),
    "Canal+ Sport 3 HD": ("canal-sport-3-hd-676", "CanalPlusSport3HD.pl"),
    "Canal+ Sport 4 HD": ("canal-sport-4-hd-677", "CanalPlusSport4HD.pl"),
    "Canal+ Sport 5 HD": ("nsport-hd-17", "CanalPlusSport5HD.pl"),
    "Xtreme TV": ("super-tv-690", "XtremeTV.pl"),
    "FightKlub HD": ("fightklub-hd-168", "FightKlubHD.pl"),
    "FightBox HD": ("fightbox-hd-453", "FightBoxHD.pl"),
    "Sport Klub HD": ("sportklub-hd-620", "SportKlubHD.pl"),
    "Sportowa TV": ("sportowatv", "SportowaTV.pl"),
    "Golf Zone": ("golf-channel-hd-554", "GolfZone.pl"),
    "TVP ABC HD": ("tvp-abc-182", "TVPABCHD.pl"),
    "TVP ABC": ("tvp-abc-182", "TVPABC.pl"),
    "2x2 HD": ("2x2-hd-613", "2x2HD.pl"),
    "Top Kids HD": ("top-kids-hd-224", "TopKidsHD.pl"),
    "Junior Music HD": ("top-kids-jr-hd-664", "JuniorMusicHD.pl"),
    "Disney Channel HD": ("disney-channel-hd-216", "DisneyChannelHD.pl"),
    "Disney Junior": ("disney-junior-469", "DisneyJunior.pl"),
    "Disney XD": ("disney-xd-235", "DisneyXD.pl"),
    "Cartoon Network HD": ("cartoon-network-hd-310", "CartoonNetworkHD.pl"),
    "BabyTV": ("baby-tv-285", "BabyTV.pl"),
    "BBC CBeebies": ("bbc-cbeebies-2", "BBCCBeebies.pl"),
    "Polsat JimJam": ("polsat-jimjam-89", "PolsatJimJam.pl"),
    "Nickelodeon": ("nickelodeon-42", "Nickelodeon.pl"),
    "Cartoonito HD": ("boomerang-hd-616", "CartoonitoHD.pl"),
    "Nicktoons HD": ("nicktoons-hd-631", "NicktoonsHD.pl"),
    "Nick Jr.": ("nick-jr-hd-662", "NickJr..pl"),
    "TeenNick": ("teennick", "TeenNick.pl"),
    "MiniMini+ HD": ("minimini-hd-435", "MiniMiniPlusHD.pl"),
    "Teletoon+ HD": ("teletoon-hd-438", "TeletoonPlusHD.pl"),
    "4Fun Kids": ("4fun-hits-283", "4FunKids.pl"),
    "Duck TV HD": ("ducktv-hd-151", "DuckTVHD.pl"),
    "Disco Polo Music": ("disco-polo-music-191", "DiscoPoloMusic.pl"),
    "Polo TV": ("polo-tv-135", "PoloTV.pl"),
    "Eska TV": ("eska-tv-hd-221", "EskaTV.pl"),
    "Eska Rock TV": ("hip-hop-tv-511", "EskaRockTV.pl"),
    "MTV Polska HD": ("mtv-polska-hd-557", "MTVPolskaHD.pl"),
    "Mezzo": ("mezzo-234", "Mezzo.pl"),
    "360TuneBox": ("360tunebox-hd-304", "360TuneBox.pl"),
    "4Fun Dance": ("4fun-fit-dance-244", "4FunDance.pl"),
    "4Fun TV": ("4fun-tv-269", "4FunTV.pl"),
    "Eska TV Extra": ("eska-tv-extra-597", "EskaTVExtra.pl"),
    "Kino Polska Muzyka": ("kino-polska-muzyka-426", "KinoPolskaMuzyka.pl"),
    "Mix tape HD": ("mixtape", "MixtapeHD.pl"),
    "Music Box Polska": ("music-box-hd-539", "MusicBoxPolska.pl"),
    "Nuta Gold HD": ("nuta-gold", "NutaGoldHD.pl"),
    "Nuta Gold": ("nuta-gold", "NutaGold.pl"),
    "Nuta TV HD": ("nuta-tv-hd-213", "NutaTVHD.pl"),
    "Nuta TV": ("nuta-tv-hd-213", "NutaTV.pl"),
    "Polsat Music HD": ("muzo-tv-200", "PolsatMusicHD.pl"),
    "Power TV HD": ("power-tv-hd-177", "PowerTVHD.pl"),
    "Power TV": ("power-tv-hd-177", "PowerTV.pl"),
    "Stars TV": ("stars-tv-hd-122", "StarsTV.pl"),
    "Stingray CMusic HD": ("c-music-tv-260", "StingrayCMusicHD.pl"),
    "VOX Music TV": ("vox-music-tv-193", "VOXMusicTV.pl"),
    "Radio Nowy Świat": ("radio-nowy-swiat", "RadioNowyŚwiat.pl"),
    "Radio 357": ("radio-357", "Radio357.pl"),
    "TVP 3 Białystok": ("tvp-3-bialystok-5", "TVP3Białystok.pl"),
    "TVP Wilno HD": ("tvp-wilno", "TVPWilnoHD.pl"),
    "TVS": ("tvs-hd-109", "TVS.pl"),
    "TV Trwam HD": ("tv-trwam-108", "TVTrwamHD.pl"),
    "TVT": ("tvt-500", "TVT.pl"),
    "TV Regio": ("tv-regio-679", "TVRegio.pl"),
    "Dla Ciebie TV": ("dlaciebie-tv-442", "DlaCiebieTV.pl"),
    "Twoja TV": ("twoja-tv-514", "TwojaTV.pl"),
    "Echo24": ("echo-24-687", "Echo24.pl"),
	# --- RĘCZNIE DOPASOWANE BRAKUJĄCE KANAŁY ---
    "TV Puls 2 HD": ("puls-2-hd-199", "TVPuls2HD.pl"),
    "TV Republika": ("tv-republika-18", "TVRepublika.pl"),
    "TV Republika HD": ("tv-republika-hd-16", "TVRepublikaHD.pl"),
    "Paramount": ("paramount-channel-hd-65", "Paramount.pl"),
    "Stopklatka TV": ("stopklatka-hd-186", "StopklatkaTV.pl"),
    "Polsat CI": ("ci-polsat-hd-640", "PolsatCI.pl"),
    "Investigation Discovery": ("id-hd-188", "InvestigationDiscovery.pl"),
    "Nat Geo": ("national-geographic-channel-hd-34", "NatGeo.pl"),
    "Nat Geo Wild": ("nat-geo-wild-hd-121", "NatGeoWild.pl"),
    "Polsat Sport Prem 1": ("polsat-sport-premium-1-643", "PolsatSportPrem1.pl"),
    "Polsat Sport Prem 2": ("polsat-sport-premium-2-644", "PolsatSportPrem2.pl"),
    "Polsat Sport Prem 1 (v2)": ("polsat-sport-premium-1-643", "PolsatSportPrem1v2.pl"),
    "Polsat Sport Prem 2 (v2)": ("polsat-sport-premium-2-644", "PolsatSportPrem2v2.pl"),
    "Extreme Sports HD": ("extreme-sports-channel-hd-217", "ExtremeSportsHD.pl"),
    "CNN International HD (ENG)": ("cnn-258", "CNNInternationalHD.pl"),
    "BBC World News Europe HD (ENG)": ("bbc-world-news-254", "BBCWorldNewsEuropeHD.pl"),
    "CNBC Europe HD (ENG)": ("cnbc-247", "CNBCEuropeHD.pl"),
    "Museum 4K": ("museum-tv-4k", "Museum4K.pl"),
    "InUltra TV UHD PL": ("insight-tv-uhd-682", "InUltraTVUHDPL.pl"),
    "TV Toya": ("toya-467", "TVToya.pl"),
    "Jazz TV HD": ("jazz-650", "JazzTVHD.pl"),
	# --- KOLEJNE DOPASOWANIA (Kanały poboczne i pokrewne nazwy) ---
    "TVP Kultura 2 HD": ("tvp-kultura-hd-680", "TVPKultura2HD.pl"),
    "TVP Historia 2": ("tvp-historia-74", "TVPHistoria2.pl"),
    "TVP ABC 2 HD": ("tvp-abc-182", "TVPABC2HD.pl"),
    "Duck TV Plus": ("ducktv-hd-151", "DuckTVPlus.pl"),
    "Fast FunBox 2": ("fast-funbox-hd-104", "FastFunBox2.pl"),
    "FilmBox ArtHouse 2": ("filmbox-arthouse-hd-190", "FilmBoxArtHouse2.pl"),
    # Opcjonalnie (jeśli chcesz podpiąć przybliżony program pod kanały walki):
    "Fight Sports HD": ("fighttime", "FightSportsHD.pl"),
    "Prime Fight HD": ("fighttime", "PrimeFightHD.pl"),
    "Canal+ Extra 1": ("3_377", "CanalPlusExtra1.pl"),
    "Viaplay Sports 1": ("3_1601", "ViaplaySports1.pl"),
    "Viaplay Sports 2": ("3_665", "ViaplaySports2.pl"),
}

# Wklej brakujące kanały do pobrania z zewnątrz: "Nazwa": ("oryginalne_id_z_zewnatrz", "ID.pl")
CHANNELS_EXTERNAL = {
    
}

# --- INICJALIZACJA BAZY DANYCH ---
def setup_db():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS cache_opisow
                 (url TEXT PRIMARY KEY, description TEXT, category TEXT, rating TEXT, age TEXT, acts TEXT, dirs TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS audycje_xml
                 (pk_id TEXT PRIMARY KEY, channel_id TEXT, start_iso TEXT, xml_data TEXT)''')
    
    # Czyszczenie archiwum ze starych audycji
    cutoff_date = datetime.datetime.now() - datetime.timedelta(days=DNI_CATCHUP)
    cutoff_iso = cutoff_date.isoformat()
    # Zakładamy alfabetyczne sortowanie formatów dat, co w przypadku ISO i XMLTV YYYYMMDD zadziała poprawnie
    c.execute("DELETE FROM audycje_xml WHERE start_iso < ?", (cutoff_iso,))
    
    conn.commit()
    conn.close()

# --- FUNKCJE POMOCNICZE (ONET) ---
def format_xmltv_date(iso_string):
    try:
        dt = datetime.datetime.fromisoformat(iso_string)
        return dt.strftime('%Y%m%d%H%M%S %z')
    except:
        return iso_string

def get_deep_details(url):
    """Głębokie skanowanie z wykorzystaniem pamięci cache w bazie danych (Bez zmian logiki)."""
    full_url = f"https://programtv.onet.pl{url}"
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("SELECT * FROM cache_opisow WHERE url=?", (full_url,))
    row = c.fetchone()
    
    if row:
        conn.close()
        return row[3], json.loads(row[5]), json.loads(row[6]), row[1], row[4]
    
    try:
        r = requests.get(full_url, timeout=10)
        s = BeautifulSoup(r.text, 'html.parser')
        
        f_desc = s.find('p', class_='entryDesc')
        f_desc = f_desc.text.strip() if f_desc else ""
        
        acts, dirs = [], []
        for li in s.select('.cast li'):
            if 'Reżyseria:' in li.text:
                nxt = li.find_next_sibling('li')
                if nxt: dirs = [x.strip() for x in nxt.text.split(',')]
            elif 'Obsada:' in li.text:
                nxt = li.find_next_sibling('li')
                if nxt: acts = [x.strip() for x in nxt.text.split(',')]
                
        rate = ""
        stars_span = s.find('span', class_=re.compile(r'stars\d+'))
        if stars_span:
            match = re.search(r'stars(\d+)', stars_span['class'][-1])
            if match: rate = f"{match.group(1)}/5"
            
        age = ""
        age_span = s.find('span', class_=re.compile(r'pegi\d+'))
        if age_span:
            match = re.search(r'pegi(\d+)', age_span['class'][-1])
            if match: age = match.group(1)

        cat = s.find('span', class_='type')
        cat = cat.text.strip() if cat else ""

        c.execute("INSERT OR REPLACE INTO cache_opisow VALUES (?, ?, ?, ?, ?, ?, ?)",
                  (full_url, f_desc, cat, rate, age, json.dumps(acts), json.dumps(dirs)))
        conn.commit()
        conn.close()
        return rate, acts, dirs, f_desc, age
    except:
        conn.close()
        return "", [], [], "", ""

# --- POBIERANIE ONETU (WIELOWĄTKOWE) ---
def process_onet_channel(channel_name, onet_slug, m3u_id):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    now_hour = datetime.datetime.now().hour
    shift = 1 if 0 <= now_hour < 4 else 0

    print(f"Pobieranie Onet: {channel_name}...")
    
    for day_offset in DNI_DO_POBRANIA:
        d = day_offset + shift
        url = f"https://programtv.onet.pl/program-tv/{onet_slug}?dzien={d}"
        
        try:
            resp = requests.get(url, timeout=15)
            soup = BeautifulSoup(resp.text, 'html.parser')
            
            iso_times = {}
            json_script = soup.find('script', type='application/ld+json')
            if json_script:
                data = json.loads(json_script.string)
                if 'itemListElement' in data:
                    for item in data['itemListElement']:
                        if 'item' in item and 'url' in item['item']:
                            prog_url = item['item']['url'].replace("https://programtv.onet.pl", "")
                            iso_times[prog_url] = item['item'].get('startDate')

            for li in soup.select('ul > li.fltrMovie, ul > li.fltrSerie, ul > li.fltrOther'):
                a_tag = li.find('a')
                if not a_tag: continue
                
                title = a_tag.text.strip()
                p_url = a_tag['href'].split('?')[0]
                
                start_iso = None
                for key in iso_times.keys():
                    if p_url in key:
                        start_iso = iso_times[key]
                        break
                if not start_iso: continue 

                start_formatted = format_xmltv_date(start_iso)
                cat = li.find('span', class_='type')
                cat = cat.text.strip() if cat else ""
                desc_tag = li.find('p')
                desc = desc_tag.text.strip() if desc_tag else ""
                
                rate, acts, dirs, age = "", [], [], ""
                if DEEP_SCAN and p_url:
                    rate, acts, dirs, f_desc, age = get_deep_details(p_url)
                    if f_desc: desc = f_desc

                xml_node = f'  <programme start="{start_formatted}" channel="{m3u_id}">\n'
                xml_node += f'    <title lang="pl">{title}</title>\n'
                if desc: xml_node += f'    <desc lang="pl">{desc}</desc>\n'
                if cat: xml_node += f'    <category lang="pl">{cat}</category>\n'
                if age: xml_node += f'    <rating system="advisory"><value>{age}</value></rating>\n'
                if acts or dirs:
                    xml_node += '    <credits>\n'
                    for d in dirs: xml_node += f'      <director>{d}</director>\n'
                    for a in acts: xml_node += f'      <actor>{a}</actor>\n'
                    xml_node += '    </credits>\n'
                if rate: xml_node += f'    <star-rating><value>{rate}</value></star-rating>\n'
                xml_node += f'  </programme>\n'

                pk_id = f"{m3u_id}_{start_iso}"
                c.execute("INSERT OR REPLACE INTO audycje_xml VALUES (?, ?, ?, ?)",
                          (pk_id, m3u_id, start_iso, xml_node))
            conn.commit()
            time.sleep(0.1) 
        except Exception as e:
            pass 
    conn.close()

# --- POBIERANIE ZEWNĘTRZNEGO EPG ---
def process_external_epg():
    if not CHANNELS_EXTERNAL:
        return
        
    print(f"\n[INFO] Pobieranie zewnętrznego pliku EPG: {EXTERNAL_EPG_URL} ...")
    try:
        resp = requests.get(EXTERNAL_EPG_URL, timeout=30)
        resp.raise_for_status()
        
        print("[INFO] Przetwarzanie i integracja zewnętrznego EPG...")
        root = ET.fromstring(resp.content)
        
        # Słownik odwrotny do szybkiego wyszukiwania: { "zewnetrzne_id": ("Nazwa Kanału", "MojeID.pl") }
        ext_to_m3u = {ids[0]: (nazwa, ids[1]) for nazwa, ids in CHANNELS_EXTERNAL.items()}
        
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        
        added = 0
        for prog in root.findall('programme'):
            ext_channel_id = prog.get('channel')
            if ext_channel_id in ext_to_m3u:
                _, m3u_id = ext_to_m3u[ext_channel_id]
                start_raw = prog.get('start', '')
                
                # Podmiana atrybutu channel na nasze ujednolicone ID
                prog.set('channel', m3u_id)
                # Zrzut bloku z powrotem do formatu tekstowego
                xml_node = ET.tostring(prog, encoding="unicode", method="xml")
                # Formatowanie do czytelności
                xml_node = "  " + xml_node.replace("><", ">\n    <").replace("</programme>", "\n  </programme>\n")
                
                pk_id = f"{m3u_id}_{start_raw}"
                c.execute("INSERT OR REPLACE INTO audycje_xml VALUES (?, ?, ?, ?)",
                          (pk_id, m3u_id, start_raw, xml_node))
                added += 1
                
        conn.commit()
        conn.close()
        print(f"[INFO] Zintegrowano {added} audycji z zewnętrznego EPG.")
    except Exception as e:
        print(f"[BŁĄD] Nie udało się pobrać lub przetworzyć zewnętrznego EPG. Pomijam. ({e})")

# --- BUDOWA OSTATECZNEGO PLIKU .XML.GZ ---
def build_xml_gz():
    print("\nBudowanie ostatecznego pliku EPG (.xml.gz)...")
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    
    with gzip.open(OUTPUT_FILE, 'wt', encoding='utf-8') as f:
        f.write('<?xml version="1.0" encoding="UTF-8"?>\n')
        f.write('<tv generator-info-name="OnetEPG-MasterBuilder">\n')
        
        # Wypisywanie nagłówków wszystkich stacji
        all_channels = {**CHANNELS_ONET, **CHANNELS_EXTERNAL}
        for nazwa, (_, m3u_id) in all_channels.items():
            f.write(f'  <channel id="{m3u_id}">\n')
            f.write(f'    <display-name>{nazwa}</display-name>\n')
            f.write(f'    <catchup-days>{DNI_CATCHUP}</catchup-days>\n')
            f.write(f'  </channel>\n')
            
        c.execute("SELECT xml_data FROM audycje_xml ORDER BY channel_id, start_iso")
        for row in c.fetchall():
            f.write(row[0])
            
        f.write('</tv>\n')
        
    conn.close()
    print(f"Plik EPG zapisano pomyślnie jako: {OUTPUT_FILE}")

# --- URUCHOMIENIE ---
if __name__ == "__main__":
    start_time_pomiar = time.time()
    
    setup_db()
    
    # 1. Baza Onetu
    if CHANNELS_ONET:
        print(f"[INFO] Rozpoczynam pobieranie wielowątkowe ONET (Wątki: {MAX_WATKOW})...")
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WATKOW) as executor:
            futures = [executor.submit(process_onet_channel, nazwa, ids[0], ids[1]) for nazwa, ids in CHANNELS_ONET.items()]
            concurrent.futures.wait(futures)
            
    # 2. Zewnętrzna Baza
    process_external_epg()
        
    # 3. Finalny Zapis
    build_xml_gz()

    end_time_pomiar = time.time()
    czas_trwania = end_time_pomiar - start_time_pomiar
    minuty = int(czas_trwania // 60)
    sekundy = int(czas_trwania % 60)
    print(f"\n[INFO] Zakończono sukcesem! Całkowity czas: {minuty} minut i {sekundy} sekund.")
