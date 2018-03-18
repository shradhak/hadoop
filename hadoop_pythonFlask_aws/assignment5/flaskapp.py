from flask import Flask,render_template,request
app = Flask(__name__)


@app.route('/')
def readfile():
    #file=open("C:\Users\Brinda\Desktop\d3\quakesmr.csv")
    file=open("\home\ubuntu\quakesmr.csv")

    result=file.read()
    return render_template('index.html',results=str(result))





if __name__ == '__main__':
    app.run()
