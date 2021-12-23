import argparse
from datetime import datetime
import psycopg2
from psycopg2 import extras
from psycopg2 import sql

params_dic = {
}
tableName = 'log_ds'

def connect():
    global params_dic
    conn = None
    try:
        conn = psycopg2.connect(**params_dic)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    return conn

def recordExtractor(mallId, file, analitica):
    global tableName
    recordInsert = []
    print(analitica)
       
    try:
        print("Recovering file record...")
        recordFromFile=[]
        searchDate = False
        for line in reversed(list(open(file))):
            lineSplit = line.rstrip().split(" ")
            if  '**PERF:' in lineSplit:
                if 'FPS' in lineSplit:
                    continue
                fps_sources = []
                for fps in line.rstrip().replace("**PERF:","").strip().split("\t"):
                    fps_sources.append(fps.split(" ")[0])
                searchDate = True
            elif searchDate:
                fps_sources.append(datetime.strptime(line.rstrip(), "%c"))
                if len(fps_sources)>1:
                    recordFromFile.append(fps_sources)
                searchDate = False                    
    except:
        print('Problem whit open'+file)
        
    if recordFromFile:
        sourceNumber = len(recordFromFile[0])-1
        lastTime = [datetime(2021, 1, 1)]*sourceNumber
        try:
            print("Recovering last time to commit...")
            conn = connect()
            if conn is None:
                raise ValueError('Error when trying to connect to the DB ...')
            cur = conn.cursor()
            for i in range(sourceNumber):
                textQuery = f"select fecha, hora from {tableName} where id_cc={mallId} and id_ds={i} and analitica='{analitica}' order by registro_id desc limit 1"
                cur.execute(textQuery)
                records = cur.fetchall()
                if records:
                    lastTime[i] = datetime.strptime(records[0][0]+' '+records[0][1],'%d-%m-%Y  %H-%M-%S')
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        finally:
            if conn is not None:
                conn.close()     
            
        print("Comparing times and preparing inserts...")
        for rec in reversed(recordFromFile):
            for source in range(sourceNumber):
                if rec[-1] > lastTime[source]:
                    recordInsert.append((mallId, source,rec[source], rec[-1].strftime("%d-%m-%Y"), rec[-1].strftime("%H-%M-%S"), analitica))
                
        print("Finish,", len(recordInsert), "records")
        
    return recordInsert

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Program that reports metrics from the computer to Slack')
    parser.add_argument('-n', '--mallNumber', type=int, required=True, help='name of the mall to report')
    parser.add_argument('-f', '--logFlujoDs', type=str, required=True, help='Absolute address of the flujo Ds log')
    parser.add_argument('-a', '--logAforoDs', type=str, required=True, help='Absolute address of the Aforo Ds log')
    args = parser.parse_args()
    
    records = recordExtractor(args.mallNumber, args.logFlujoDs, 'flujo')
    records += recordExtractor(args.mallNumber, args.logAforoDs, 'aforo')
    
    if records:
        queryText = "INSERT INTO {table}(id_cc, id_ds, fps, fecha, hora, analitica) VALUES %s;"
        try:
            conn = connect()
            if conn is None:
                raise ValueError('Error when trying to connect to the DB ...')
            cur = conn.cursor()
            sqlQueryFlujo = sql.SQL(queryText).format(table=sql.Identifier(tableName))
            extras.execute_values(cur, sqlQueryFlujo.as_string(cur), records)
            conn.commit()
            
            print(str(cur.rowcount)+" records inserted successfully")
        except (Exception, psycopg2.DatabaseError) as error:
                print(error)
        finally:
            if conn is not None:
                conn.close()
    else:
        print("Whitout data")
