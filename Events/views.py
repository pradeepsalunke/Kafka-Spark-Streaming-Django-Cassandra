from datetime import datetime

from django.shortcuts import render


def index(request):
    current_user="ballam"

    return render(request,'home.html',{'date':datetime.now(),'login':current_user})