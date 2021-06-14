import pandas as pd
import requests
import xml.etree.ElementTree as ET
from lxml import etree
from collections import Counter
import gspread
from gspread_dataframe import set_with_dataframe

response_USA = requests.get('https://storage.googleapis.com/tarea-4.2021-1.tallerdeintegracion.cl/gho_USA.xml')
response_DEU = requests.get('https://storage.googleapis.com/tarea-4.2021-1.tallerdeintegracion.cl/gho_DEU.xml')
response_NOR = requests.get('https://storage.googleapis.com/tarea-4.2021-1.tallerdeintegracion.cl/gho_NOR.xml')
response_FRA = requests.get('https://storage.googleapis.com/tarea-4.2021-1.tallerdeintegracion.cl/gho_FRA.xml')
response_JPN = requests.get('https://storage.googleapis.com/tarea-4.2021-1.tallerdeintegracion.cl/gho_JPN.xml')
response_CAN = requests.get('https://storage.googleapis.com/tarea-4.2021-1.tallerdeintegracion.cl/gho_CAN.xml')


root = ET.XML(response_USA.content)
root2 = ET.XML(response_DEU.content)
root3 = ET.XML(response_NOR.content)
root4 = ET.XML(response_FRA.content)
root5 = ET.XML(response_JPN.content)
root6 = ET.XML(response_CAN.content)

validos = ['GHO', 'YEAR', 'SEX', 'COUNTRY', 'AGEGROUP', 'GHECAUSES', 'Display', 'Numeric', 'Low', 'High']

indicadores_muerte = ['Number of deaths', 'Number of infant deaths', 'Number of under-five deaths',
              'Mortality rate for 5-14 year-olds (probability of dying per 1000 children aged 5-14 years)',
              'Adult mortality rate (probability of dying between 15 and 60 years per 1000 population)',
              'Estimates of number of homicides', 'Crude suicide rates (per 100 000 population)', 
              'Mortality rate attributed to unintentional poisoning (per 100 000 population)', 
              'Number of deaths attributed to non-communicable diseases, by type of disease and sex',
              'Estimated road traffic death rate (per 100 000 population)',
              'Estimated number of road traffic deaths']

indicadores_peso = ['Mean BMI (kg/m&#xb2;) (crude estimate)', 'Mean BMI (kg/m&#xb2;) (age-standardized estimate)',
                   'Prevalence of obesity among adults, BMI &GreaterEqual; 30 (age-standardized estimate) (%)',
                   'Prevalence of obesity among children and adolescents, BMI > +2 standard deviations above the median (crude estimate) (%)',
                   'Prevalence of overweight among adults, BMI &GreaterEqual; 25 (age-standardized estimate) (%)',
                   'Prevalence of overweight among children and adolescents, BMI > +1 standard deviations above the median (crude estimate) (%)',
                   'Prevalence of underweight among adults, BMI < 18.5 (age-standardized estimate) (%)',
                   'Prevalence of thinness among children and adolescents, BMI < -2 standard deviations below the median (crude estimate) (%)']

indicadores_otros = ['Alcohol, recorded per capita (15+) consumption (in litres of pure alcohol)',
                    'Estimate of daily cigarette smoking prevalence (%)',
                    'Estimate of daily tobacco smoking prevalence (%)',
                    'Estimate of current cigarette smoking prevalence (%)',
                    'Estimate of current tobacco smoking prevalence (%)',
                    'Mean systolic blood pressure (crude estimate)',
                    'Mean fasting blood glucose (mmol/l) (crude estimate)',
                    'Mean Total Cholesterol (crude estimate)']

full_data = []

for child in root:
    data = {'GHO': None, 'YEAR': None, 'SEX': None, 'COUNTRY': None, 'AGEGROUP': None, 'GHECAUSES': None, 'Display': None, 'Numeric': None, 'Low': None, 'High': None}
    gho_text = [None]
    for subchild in child:
        if subchild.tag == 'GHO':
            gho_text[0] = (subchild.text)
        if subchild.tag in validos:
            data[subchild.tag] = subchild.text
        if subchild.tag == 'Numeric' or subchild.tag == 'Low' or subchild.tag == 'High':
            if subchild.text != None:
                data[subchild.tag] = float(subchild.text)
    if gho_text[0] in indicadores_muerte or gho_text[0] in indicadores_peso or gho_text[0] in indicadores_otros:
        full_data.append(data)
        
for child in root2:
    data = {'GHO': None, 'YEAR': None, 'SEX': None, 'COUNTRY': None, 'AGEGROUP': None, 'GHECAUSES': None, 'Display': None, 'Numeric': None, 'Low': None, 'High': None}
    gho_text = [None]
    for subchild in child:
        if subchild.tag == 'GHO':
            gho_text[0] = (subchild.text)
        if subchild.tag in validos:
            data[subchild.tag] = subchild.text
        if subchild.tag == 'Numeric' or subchild.tag == 'Low' or subchild.tag == 'High':
            if subchild.text != None:
                data[subchild.tag] = float(subchild.text)
    if gho_text[0] in indicadores_muerte or gho_text[0] in indicadores_peso or gho_text[0] in indicadores_otros:
        full_data.append(data)
        
for child in root3:
    data = {'GHO': None, 'YEAR': None, 'SEX': None, 'COUNTRY': None, 'AGEGROUP': None, 'GHECAUSES': None, 'Display': None, 'Numeric': None, 'Low': None, 'High': None}
    gho_text = [None]
    for subchild in child:
        if subchild.tag == 'GHO':
            gho_text[0] = (subchild.text)
        if subchild.tag in validos:
            data[subchild.tag] = subchild.text
        if subchild.tag == 'Numeric' or subchild.tag == 'Low' or subchild.tag == 'High':
            if subchild.text != None:
                data[subchild.tag] = float(subchild.text)
    if gho_text[0] in indicadores_muerte or gho_text[0] in indicadores_peso or gho_text[0] in indicadores_otros:
        full_data.append(data)
        
for child in root4:
    data = {'GHO': None, 'YEAR': None, 'SEX': None, 'COUNTRY': None, 'AGEGROUP': None, 'GHECAUSES': None, 'Display': None, 'Numeric': None, 'Low': None, 'High': None}
    gho_text = [None]
    for subchild in child:
        if subchild.tag == 'GHO':
            gho_text[0] = (subchild.text)
        if subchild.tag in validos:
            data[subchild.tag] = subchild.text
        if subchild.tag == 'Numeric' or subchild.tag == 'Low' or subchild.tag == 'High':
            if subchild.text != None:
                data[subchild.tag] = float(subchild.text)
    if gho_text[0] in indicadores_muerte or gho_text[0] in indicadores_peso or gho_text[0] in indicadores_otros:
        full_data.append(data)
        
for child in root5:
    data = {'GHO': None, 'YEAR': None, 'SEX': None, 'COUNTRY': None, 'AGEGROUP': None, 'GHECAUSES': None, 'Display': None, 'Numeric': None, 'Low': None, 'High': None}
    gho_text = [None]
    for subchild in child:
        if subchild.tag == 'GHO':
            gho_text[0] = (subchild.text)
        if subchild.tag in validos:
            data[subchild.tag] = subchild.text
        if subchild.tag == 'Numeric' or subchild.tag == 'Low' or subchild.tag == 'High':
            if subchild.text != None:
                data[subchild.tag] = float(subchild.text)
    if gho_text[0] in indicadores_muerte or gho_text[0] in indicadores_peso or gho_text[0] in indicadores_otros:
        full_data.append(data)
        
for child in root6:
    data = {'GHO': None, 'YEAR': None, 'SEX': None, 'COUNTRY': None, 'AGEGROUP': None, 'GHECAUSES': None, 'Display': None, 'Numeric': None, 'Low': None, 'High': None}
    gho_text = [None]
    for subchild in child:
        if subchild.tag == 'GHO':
            gho_text[0] = (subchild.text)
        if subchild.tag in validos:
            data[subchild.tag] = subchild.text
        if subchild.tag == 'Numeric' or subchild.tag == 'Low' or subchild.tag == 'High':
            if subchild.text != None:
                data[subchild.tag] = float(subchild.text)
    if gho_text[0] in indicadores_muerte or gho_text[0] in indicadores_peso or gho_text[0] in indicadores_otros:
        full_data.append(data)
    
df = pd.DataFrame(full_data)

# ACCES GOOGLE SHEET
gc = gspread.service_account(filename='tarea-4-iic3103-316623-83ce3383287c.json')
sh = gc.open_by_key('10E5uc716DIbNNwdT8ZhEULF93sNvIuv6bMDdePHm5G8')
worksheet = sh.get_worksheet(0)
set_with_dataframe(worksheet, df)
