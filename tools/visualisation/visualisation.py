import argparse
import os
import sys

from flask import Flask, request, render_template
from flask_wtf import FlaskForm
from wtforms import SelectField, SubmitField

import config
import fieldRanking
import hourlyFieldValue

from postprocess import processdata

parser = argparse.ArgumentParser(description = "This script starts a server on localhost:5000 "
								+ "that displays charts based on the processed files")
parser.add_argument("--monthsFolder", "-m", default=config.monthsFolder,
	                type=str, help="The folder in which the months directories "
	                + "are residing.")

args = parser.parse_args()

class chartForm(FlaskForm):
	month = SelectField("Month")
	chartType = SelectField("Chart Type", choices = [("fR", "Field Ranking"), ("hC", "Hourly Count")])
	field = SelectField("Field")

class availableFieldsHandler():

	keys = list()

	def handle(self, sparqlQuery, processed):
		self.keys = processed.keys()

app = Flask(__name__)
app.secret_key = 'development key'

@app.route('/', methods = ['GET', 'POST'])
def selectChart():
	form = chartForm()

	try:
		form.month.choices = [(d, d) for d in sorted(os.walk(args.monthsFolder).next()[1])]
		form.month.default = form.month.choices[0][0]
	except StopIteration:
		return render_template("error.html")

	handler = availableFieldsHandler()

	if form.month.data == u"None":
		selectedMonth = form.month.choices[0][0]
	else:
		selectedMonth = form.month.data

	if form.chartType.data == u"None":
		selectedChartType = form.chartType.choices[0][0]
	else:
		selectedChartType = form.chartType.data

	processdata.processDay(handler, 1, selectedMonth, args.monthsFolder, endIdx=0)

	try:
		form.field.choices = [(d, d) for d in sorted(handler.keys)]
	except StopIteration:
		return render_template("error.html")

	if form.field.data == u"None":
		selectedField = form.field.choices[0][0]
	else:
		selectedField = form.field.data

	if selectedChartType == "fR":
		result = fieldRanking.fieldRanking(selectedMonth, selectedField, monthsFolder = args.monthsFolder)
		labels = []
		values = []
		for key, value in result.iteritems():
		    labels.append(key)
		    values.append(value)
		return render_template('selectChart.html', form = form, values=values, labels=labels)
	if selectedChartType == "hC":
		result = hourlyFieldValue.hourlyFieldValue(selectedMonth, selectedField, monthsFolder = args.monthsFolder)
		
		return render_template('selectChart.html', form = form)

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5000)
