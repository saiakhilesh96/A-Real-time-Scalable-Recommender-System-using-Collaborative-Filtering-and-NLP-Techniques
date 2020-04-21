from flask import Flask,render_template,request
import MySQLdb
#import pymysql

app= Flask(__name__,'/templates')
#conn= MySQLdb.connect('localhost','root','sairam','RECSYS')
#conn.query('SELECT * FROM PRODUCTS LIMIT 10')
#cur= conn.cursor()
#cur.execute("SELECT * FROM PRODUCTS LIMIT 10")
#data= cur.fetchall()
@app.route('/')
def home():
    return render_template('home.html')

@app.route('/recs',methods = ['GET','POST'])
def result():
    if request.method == 'POST':
        import pymysql
        nm= request.form['nm']
        print("The values are ",nm)
        print(type(nm))
        #nm= int (nm)
        conn= pymysql.connect('localhost','root','sairam','RECSYS')
        print("Connected to database successfully")
        cur= conn.cursor()
        #qry= "SELECT * FROM PRODUCTS WHERE pro_id= "+nm
        qry= "SELECT b.user,a.rec_prods FROM ALS AS a INNER JOIN USERS AS b ON a.user_id = b.user_id WHERE b.user_id="+nm
        #print(qry)
        cur.execute(qry)
        data= cur.fetchall()
        rec_prods_list= data[0][1]
        #print(type(rec_prods_list))
        rec_prods_list= rec_prods_list.replace('[','(')
        rec_prods_list= rec_prods_list.replace(']',')')
        #print("The recommended products ",rec_prods_list)
        qry= "SELECT product FROM PRODUCTS WHERE pro_id IN "+rec_prods_list
        cur.execute(qry)
        data_prods= cur.fetchall()
        print(data_prods)
        
        
        rec_prods= []
        for t in data_prods:
            rec_prods.append(t[0])
        rec_prods= str(rec_prods)
        rec_prods= rec_prods.replace('[','(')
        rec_prods= rec_prods.replace(']',')')

        #print(rec_prods)
        qry= "SELECT movie,title FROM TITLES WHERE movie IN "+rec_prods
        cur.execute(qry)
        data_final= cur.fetchall()
        print("Movies with titles ",data_final)
        #sairam
        
        #Here small logic
        d1=  {i: "TITLE NOT AVAILABLE" for (i,) in data_prods}
        d2= {i:j for (i,j) in data_final}
        for movie in d2:
            d1[movie]= d2[movie]

        print(type(d1))
    return render_template('fetch.html',data= d1)    



if __name__ == "__main__":
    app.run(host='0.0.0.0',debug=True)