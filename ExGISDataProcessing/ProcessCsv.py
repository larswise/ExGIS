import csv, psycopg2

class DataParser:
    def __init__(self, host, database, user, password):
        self.conn = psycopg2.connect("dbname='{}' user='{}' host='{}' password='{}'".format(database, user, host, password))
        pass

    def ProcessFile(self, fileName, destinationColumns, destinationTable, delim, quotech, coordinateIndexes, hasHeader):
        with open(fileName, 'rb') as csvfile:
            if hasHeader:
                next(csvfile)
            rdr = csv.reader(csvfile, delimiter = delim, quotechar = quotech)
            cursor = self.conn.cursor()
            for row in rdr:
                try:
                    inserts = [str("'" + c + "'") if i not in coordinateIndexes else str(c) for i, c in enumerate(row)]

                    statement = "INSERT INTO {} ({}) VALUES({})".format(destinationTable, destinationColumns, ",".join(inserts))
                    print statement
                    cursor.execute(statement)
                    self.conn.commit()
                except Exception as e:
                    print e.message
        pass

    def InsertIntoPg(self, row, columns):

        pass

def main():
    cols = "Location,Cites,Perpetrator,Weapon,Injuries,Fatalities,Description,Latitude,Longitude,Time,RegionId"
    parser = DataParser("localhost", "Terrorism", "postgres", "xXxXxXxXxXxXx")
    parser.ProcessFile("L:\\New folder\\ObservationData_fyefxib\\ObservationData_fyefxib.csv", cols, "Observations", ",", "\"", [7,8], True)

if __name__ == "__main__": main()
