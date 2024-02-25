
import geopandas as gpd, pandas as pd
import requests, re
from sqlalchemy import create_engine, Column, Integer, Numeric, Text, DateTime
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from sqlalchemy.orm import declarative_base, sessionmaker
from shapely.geometry import Point


def get_links():
    adres_url = f'https://liveuamap.com/pl'
    res = requests.get(adres_url)
    res_html = BeautifulSoup(res.text, 'html.parser')

    div_elements = res_html.find_all('div', {'data-link': True})

    url_list = []

    for div in div_elements:
        data_link_value = div.get('data-link')
        url_list.append(data_link_value)
    return url_list

class html_data:

    def __init__(self, url):
        self.url = url

    def get_lat_lng(self):
        res = requests.get(self.url)
        res_html = BeautifulSoup(res.text, 'html.parser')
        script_block = res_html.find('script', string=re.compile('(lat|lng)=.+;'))

        lat_match = re.search(r'lat=(-?\d+\.\d+);', script_block.text)
        lng_match = re.search(r'lng=(-?\d+\.\d+);', script_block.text)

        _lat = lat_match.group(1)
        _lng = lng_match.group(1)
        print(f"Szerokość geograficzna: {_lat}")
        print(f"Długość geograficzna: {_lng}")
        return _lat, _lng

    def get_timestamp(self):
        res = requests.get(self.url)
        res_html = BeautifulSoup(res.text, 'html.parser')
        date_add_element = res_html.find(class_='date_add')
        if date_add_element:
            date_add_text = date_add_element.get_text(strip=True)
            hours_ago: str = date_add_text.split()[0]
        else:
            print("nie ma godziny")

        current_datetime = datetime.now()
        rounded_datetime = current_datetime.replace(minute=0)
        result_datetime = rounded_datetime - timedelta(hours=int(hours_ago))
        result_timestamp = result_datetime.strftime('%Y-%m-%d %H:%M')
        print(result_timestamp)

        return result_timestamp


    def get_desc(self):
        res = requests.get(self.url)
        res_html = BeautifulSoup(res.text, 'html.parser')
        desc_ = res_html.find('h2').text

        if desc_:
            print(desc_)

        return desc_

Base = declarative_base()

class tab1(Base):
    __tablename__ = 'incident_data'

    id =Column(Integer, primary_key=True, autoincrement=True)
    lat = Column(Numeric)
    lng = Column(Numeric)
    date = Column(DateTime)
    desc = Column(Text)



class push_to_db:

    def __init__(self):
        self.engine = create_engine(f"postgresql://postgres:sad1@localhost/uamap")
        self.Session = sessionmaker(bind=self.engine)


    def connect(self, lat_, lng_, result_timestamp, desc_):
        session = self.Session()
        try:

            existing_record = session.query(tab1).filter(tab1.desc == desc_).first()

            if existing_record is None:
                data_input = tab1(
                    lat=lat_,
                    lng=lng_,
                    date=result_timestamp,
                    desc=desc_
                )

                session.add(data_input)
                session.commit()
            else:
                print("Rekord z takim samym opisem już istnieje, nie został zapisany.")
        except Exception as error:
            print(error)
        finally:
            session.close()
            print('Koniec')


def prepare_gpkg_from( output_name: str) -> None:
    connection = "postgresql://postgres:sad1@localhost:5432/uamap"
    engine = create_engine(connection)

    Session = sessionmaker(bind=engine)
    session = Session()

    # glowne zapytanie, grupujące 3 atrybuty z tab2 i 2 z tab1
    _query = session.query(tab1.lat, tab1.lng, tab1.date, tab1.desc)

    # wykonanie zapytania i zapis do zmiennej dataframe
    df = pd.read_sql_query(_query.statement, engine)

    session.close()

    # tworzenie geometrii z atrybutow lng i lat
    # zip iteruje po dwoch listach jednoczesnie i laczy te elementy ze soba
    geometry = [Point(x, y) for x, y in zip(df['lng'], df['lat'])]

    # dataframe z geometrią
    gdf = gpd.GeoDataFrame(df, geometry=geometry, crs="EPSG:4326")

    # zapis do pliku
    gdf.to_file(f'{output_name}.gpkg', driver='GPKG')
