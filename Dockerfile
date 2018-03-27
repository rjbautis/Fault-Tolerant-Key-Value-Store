FROM python:3-onbuild

RUN pip3 install -r requirements.txt

EXPOSE 8080

CMD ["python3", "hw4solution.py"]