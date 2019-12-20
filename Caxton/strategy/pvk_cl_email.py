import win32com.client as win32
import datetime as dt

import smtplib
from email.mime.application import MIMEApplication
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
from email.message import EmailMessage
from email.mime.text import MIMEText
from io import BytesIO
from os.path import basename

# panormus imports
import panormus.utils.chrono as puc

# utils_pvk imports
import utils_pvk.lib_string_fns as string_fns
import utils_pvk.lib_date_fns as date_fns


def process_recipient_arg(recipients):
    """
    :param str|iterable[str] recipients: comma separated. semicolon separated string, or iterable of strings.
    :return: comma-separated string of recipients
    """
    if not recipients:
        return ''
    elif isinstance(recipients, str):
        return recipients.replace(';', ',')
    else:
        return ','.join(recipients)


class Email:
    def __init__(self,
                 to="pvklooster@caxton.com",
                 subject="email subject",
                 body="error: could not read html email. please enable html to view this email or read in an html-compatible email client.",
                 htmlbody="html email body",
                 from_email="pvklooster@caxton.com",
                 cc='',
                 bcc=''):

        self.from_email = from_email
        self.to = to
        self.cc = cc
        self.bcc = bcc
        self.subject = subject
        self.htmlbody = htmlbody
        self.nohtml_body = body
        self.attachments = []
        self.inline_images = []
        self.msg = None

    def add_attachment(self, path):
        self.attachments.append(path)

    @staticmethod
    def initial_html():
        html = """<!--[if mso]>
        <style type="text/css">
            body, table, td {font-family: Calibri, sans-serif !important;, font-size:11pt !important}
        </style>
        <![endif]-->
        <div style="font-family: Calibri; font-size: 11pt;">
        """
        return html

    def add_inline_image(self, html_body, image_path):
        # image_cid = string_fns.hash(image_path)

        image_cid = (str(puc.now())).replace(' ', '')

        html_body += "<img src=\"cid:{0}\">".format(image_cid)

        with open(image_path, 'rb') as fp:
            msgImage = MIMEImage(fp.read())

        msgImage.add_header('Content-ID', image_cid)
        self.inline_images.append(msgImage)

        return html_body

    def add_html_body(self, htmlbody, add_autogen_msg=False):
        if add_autogen_msg:
            htmlbody += "<br><br><br>This is an automated message (generated {}utc)</i>".format(date_fns.to_str(dt.datetime.utcnow(), "dd-mmm-yyyy hh:mm"))

        htmlbody += "</div>"
        self.htmlbody = htmlbody

    def build(self):
        msg = MIMEMultipart()
        msg['Subject'] = self.subject
        msg['From'] = self.from_email
        msg['TO'] = process_recipient_arg(self.to)
        msg['CC'] = process_recipient_arg(self.cc)
       msg['BCC'] = process_recipient_arg(self.bcc)
        msg.attach(MIMEText(self.htmlbody, 'html'))

        for fpath in self.attachments or []:
            with open(fpath, "rb") as file:
                part = MIMEApplication(
                    file.read(),
                    Name=basename(fpath)
                )
            # After the file is closed
            part['Content-Disposition'] = 'attachment; filename="%s"' % basename(fpath)
            msg.attach(part)

        if self.inline_images:
            for item in self.inline_images:
                msg.attach(item)

        self.msg = msg

    def send(self):
        print("sending email")
        if self.msg is None:
            self.build()

        with smtplib.SMTP('smtp.caxton.com') as s:
            s.send_message(self.msg)


if __name__ == "__main__":
    mail = Email(to="pvklooster@caxton.com", subject="test_dual_email_addresses")
    html = mail.initial_html()
    html += "Test image <br><br>"
    path = r"H:\python_local\storage\temp\plot_2019-06-24_10-26-09_bwnaqqm1ni.png"
    html = mail.add_inline_image(html, path)
    mail.add_html_body(html, False)
    mail.send()