# kod dziala tylko do 23 godzin - tyle jest na stronie, chyba ze zacznie sie przewijac karte

from _utilities import get_links, html_data, push_to_db, prepare_gpkg_from

def main():
    url_list = get_links()
    for url in url_list:
        _data = html_data(url)
        lat, lng = _data.get_lat_lng()
        timestamp = _data.get_timestamp()
        desc = _data.get_desc()

        _db = push_to_db()
        _db.connect(lat, lng, timestamp, desc)
    output_name = input('podaj nazwe pliku gpkg')
    prepare_gpkg_from(output_name)

if __name__ == "__main__":
    main()