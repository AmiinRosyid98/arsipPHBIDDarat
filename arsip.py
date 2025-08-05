import pandas as pd
import requests
from urllib.parse import urlparse
from sqlalchemy import create_engine
import sys

#FUNGSI DOWNLOAD FOTO
def get_filename_from_url(url):
    return urlparse(url).path.split('/')[-1]

def download_file(url, destination, no, totalRows):
    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()

        with open(destination, 'wb') as file:
            for chunk in response.iter_content(chunk_size=1024):
                if chunk:
                    file.write(chunk)

        print(f"({no}/{totalRows}) SUKSES => File berhasil disimpan: {destination}")

    except requests.exceptions.RequestException as e:
        print(f"{no}/{totalRows})  GAGAL => {e}")

#FUNGSI GENERATE EXCEL
def execute_query_to_excel(host, user, password, database, start_date, end_date, excel_file_path):
    engine = create_engine(f"mysql+mysqlconnector://{user}:{password}@{host}/{database}")

    try:
        query = """
        SELECT File.Created, order_data.ID, order_data.nomor_order, penugasan_tracking_data.nopol, 
                            case 
                                    when tracking_data.multi != '0' then CONCAT(tracking_data.status, ' ',tracking_data.multi)
                            ELSE
                                    tracking_data.status
                            END
                            AS status, lelang_data.jenis_lelang, File.Name, File.FileFilename  FROM FotoTracking_Live
                            LEFT JOIN tracking_data ON tracking_data.ID = FotoTracking_Live.TrackingID
                            LEFT JOIN penugasan_tracking_data ON penugasan_tracking_data.ID = tracking_data.PenugasanTrackingID
                            LEFT JOIN order_data ON order_data.ID = penugasan_tracking_data.OrderID
                            LEFT JOIN lelang_data ON lelang_data.ID = order_data.LelangID 
                            JOIN File ON File.ID = FotoTracking_Live.ID

                            WHERE DATE(order_data.Created) >= %s AND DATE(order_data.Created) <= %s

                            ORDER BY order_data.Created DESC;
        """

        df = pd.read_sql_query(query, engine, params=(start_date, end_date))

        df.to_excel(excel_file_path, index=False)

        print("Query berhasil dijalankan, file disimpan! => {}".format(excel_file_path))
        print("==========================================================================")
    except Exception as e:
        print("Error: " + str(e))
    finally:
        # Close the SQLAlchemy engine
        engine.dispose()

def main_menu():
    print("1. Generate .xlsx backup tracking")
    print("2. Auto download foto")
    print("3. Exit")

if __name__ == "__main__":
    while True:
        main_menu()
        choice = int(input("Pilih: "))
        if choice == 1:
            start_date = input("Tanggal awal   (yyyy-mm-dd)* :")
            end_date =   input("Tanggal akhir  (yyyy-mm-dd)* :")

            # KONEKSI DATABASE
            host = "mysql-phbid-sg-new.cluster-cstciorrht3u.ap-southeast-1.rds.amazonaws.com"
            user = "phbiddarat_webc_live"
            password = "kr34t1fb1j4ks4n4"
            database = "phbiddarat_live"

            # start_date = "2021-01-27"
            # end_date = "2021-03-27"

            excel_file_path = "arsip"+start_date+" - "+end_date+".xlsx"

            execute_query_to_excel(host, user, password, database, start_date, end_date, excel_file_path)
        elif choice == 2:
            excel_file_path = './doit.xlsx'
            df = pd.read_excel(excel_file_path)
            no = 0
            totalRows = df.shape[0]

            for index, row in df.iterrows():
                no = no + 1
                
                file_url = 'https://phbiddarat.prahu-hub.com/assets/'+row['FileFilename'] #BASE LINK
                destination_path = "./dokumen_download/"+row['nomor_order']+"_"+row['status']+"_"+get_filename_from_url(file_url)

                download_file(file_url, destination_path, no, totalRows)

                print(f"file: {file_url} simpan di: {destination_path}")
        else:
            sys.exit()