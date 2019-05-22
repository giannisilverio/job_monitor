#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed May 22 14:12:52 2019

@author: gianniconceicao
"""
from flask import Flask, render_template

app = Flask(__name__)


@app.route("/")
def index():
    return("Hello World!")

@app.route("/gianni")
def gianni():
    aux = "variavel de teste"
    return render_template("teste.html", nada=aux)

if __name__ == "__main__":
    app.run(debug=True, host = "0.0.0.0")


