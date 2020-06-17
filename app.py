from flask import Flask, request
from flask_restful import Resource
from sheets_setup import main, edit_sheet1_data, insert_data_to_sheets

app = Flask(__name__)

@app.route("/")
def hello():
  return "Hello Worldddd!"

# A route to upload revevant csv files to google sheets
@app.route('/csv_post', methods=['POST'])
def upload_csv():
    if not request.files['file']:
        abort(400)
    uploaded_file = request.files['file']

    edited_sheets = main()
    new_range_name, customer_keywords = edit_sheet1_data(uploaded_file, edited_sheets)
    insert_data_to_sheets(new_range_name, customer_keywords)
    # data = pd.read_csv(file)
    return "Success!"

if __name__ == "__main__":
  app.run()