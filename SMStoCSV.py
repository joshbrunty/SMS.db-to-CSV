import sqlite3
import csv


def extract_db():
    sql_query = """
                SELECT
                    message.ROWID as 'ID',
                    chat.ROWID as 'Chat ID',
                    chat.chat_identifier as 'Chat Handle',
                    message.date as  'Date',
                    message.text as 'Message Content',
                    message.is_from_me as 'From Account Owner'
                FROM
                    message
                LEFT JOIN chat ON
                    message.handle_id = chat.ROWID
                ORDER BY
                    message.date ASC
                """

    conn = sqlite3.connect('sms.db')
    c = conn.cursor()

    with open('sms.db extraction.csv', mode = 'w', newline = '', encoding = 'utf-8') as csv_file:

        # Write the header row
        fieldnames = ['ID', 'Chat ID', 'Chat Handle', 'Date', 'Message Content', 'From Account Owner']
        writer = csv.DictWriter(csv_file, fieldnames = fieldnames)
        writer.writeheader()

        for message_id, chat_id, chat_handle, message_date, message_text, message_from_account in c.execute(sql_query):

            # Construct our data
            parsed_message = {
                'ID': message_id,
                'Chat ID': chat_id,
                'Chat Handle': chat_handle,
                'Date': message_date,
                'Message Content': message_text,
                'From Account Owner': message_from_account,
            }

            # Write our data to the file.
            writer.writerow(parsed_message)


def main():
    extract_db()


if __name__ == '__main__':
    main()
