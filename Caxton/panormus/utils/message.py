import smtplib

from email.mime.application import MIMEApplication

from email.mime.image import MIMEImage

from email.mime.multipart import MIMEMultipart

from email.message import EmailMessage

from email.mime.text import MIMEText

from io import BytesIO

from os.path import basename

from panormus.config.settings import SMTP_HOST

import panormus.utils.chrono as puc

FO_SUPPORT = 'team-it-qa@caxton.com'

MO_FIXED_INCOME = 'MoFixedIncome@caxton.com'

MO_EQUITIES = 'MOEquities@caxton.com'

RISK_TEAM = 'Risk@caxton.com'


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


def send_email_simple(

        body, subject, to,

        from_email='panormus@caxton.com', cc='', bcc='',

):
    """

    :description: Send emails.

    :param str body: plain text body

    :param str subject: string for email subject

    :param str|iterable[str] to: list or comma-separated string of email addresses.

    :param str from_email: string, email address of sender.

    :param str|iterable[str] cc: list or comma-separated string of email addresses.

    :param str|iterable[str] bcc: list or comma-separated string of email addresses.

    :return: None

    """

    msg = EmailMessage()

    msg['Subject'] = subject

    msg['From'] = from_email

    msg['TO'] = process_recipient_arg(to)

    msg['CC'] = process_recipient_arg(cc)

    msg['BCC'] = process_recipient_arg(bcc)

    msg.set_content(body)

    # Send the message via local SMTP server.

    with smtplib.SMTP(SMTP_HOST) as s:
        s.send_message(msg)


def send_email_html(

        body_html, subject, to,

        from_email='panormus@caxton.com', cc='', bcc='',

        attachment_list=None, files_to_attach=None

):
    """

    :description: Send emails.

    :param str body_html: html or plain string to put in email body.

    :param str subject: string for email subject

    :param str|iterable[str] to: list or comma-separated string of email addresses.

    :param str from_email: string, email address of sender.

    :param str|iterable[str] cc: list or comma-separated string of email addresses.

    :param str|iterable[str] bcc: list or comma-separated string of email addresses.

    :param list attachment_list: list of things to attach.

    :param list files_to_attach: list of file paths to attach.

    :return: None

    """


msg = MIMEMultipart()

msg['Subject'] = subject

msg['From'] = from_email

msg['TO'] = process_recipient_arg(to)

msg['CC'] = process_recipient_arg(cc)

msg['BCC'] = process_recipient_arg(bcc)

msg.attach(MIMEText(body_html, 'html'))

if attachment_list:

    for item in attachment_list:
        msg.attach(item)

if files_to_attach:

    for f in files_to_attach or []:
        fn = basename(f)

        with open(f, "rb") as fil:
            part = MIMEApplication(fil.read(), Name=fn)

        # After the file is closed

        part['Content-Disposition'] = 'attachment; filename="%s"' % fn

        msg.attach(part)

# Send the message via local SMTP server.

with smtplib.SMTP(SMTP_HOST) as s:
    s.send_message(msg)


def make_cid(name=''):
    '''

    :description: Generate a timestamped cid and remove illegal characters. Timestamp is used to avoid lazy image \

    matching by outlook when different emails have images with the same CID.

    :param str name: name to combine with timestamp to clarify CID.

    :rtype: str

    '''

    return (name + puc.now().strftime('%Y%m%d-%H%M%S-%f')).replace(' ', '').replace(':', '-')


def make_cid_html(cid, width_ovrd=None, height_ovrd=None):
    '''

    :description: Make html tag for given cid.

    :param str cid: content id string

    :param int width_ovrd: integer pixel override to reserve page space (only specify height OR width)

    :param int height_ovrd: integer pixel override to reserve page space (only specify height OR width)

    :rtype: str

    '''

    scale_str = 'width=%s ' % width_ovrd if width_ovrd else ''

    scale_str += 'height=%s ' % height_ovrd if height_ovrd else ''

    html = '<img %s src="cid:%s">' % (scale_str, cid)

    return html


def fig_dict_to_html(

        fig_dict,

        fig_separator='<br><br>', key_as_caption=True, tight_layout=True,

        width_ovrd=None, height_ovrd=None, fig_subtext_dict=None

):
    '''

    :description: Uses fig_dict keys (strings) and fig values to create attachable images with associated \

    names that are referenced by auto-generated html to in-line the attached images in the email body.

    :param dict fig_dict: dictionary of figures.

    :param str fig_separator: vertical separator between figures.

    :param bool key_as_caption: Show dictionary key as caption above image

    :param bool tight_layout: apply tight_layout command to each figure

    :param int width_ovrd: integer pixel override to reserve page space (only specify height OR width)

    :param int height_ovrd: integer pixel override to reserve page space (only specify height OR width)

    :param None|dict fig_subtext_dict: a dict of (html) strings to put beneath each figure with keys matching fig_dict.

    :return: dictionary with 'html', block of html with images stacked vertically, \

    'img_list' as list of images to use as attachments.

    '''

    img_list = []

    html_list = []

    for k, v in fig_dict.items():

        if tight_layout:
            v.tight_layout()

        fh = BytesIO()

        v.savefig(fh, format='png')

        img = MIMEImage(fh.getvalue())

        cid = make_cid(k)

        img.add_header('Content-ID', cid)

        img_list.append(img)

        caption = f'<div class="caption">{k}</div>' if key_as_caption else ''

        html_item = make_cid_html(cid, width_ovrd, height_ovrd)

        subtext = fig_subtext_dict.get(k, '') if fig_subtext_dict else ''

        html_list.append(caption + html_item + subtext)

    full_html = fig_separator.join(html_list)

    return {'html': full_html,

            'img_list': img_list}


def load_image_files(image_paths):
    '''

    :description: Load images from disk to MIMEImage. Returns a dict with cid keys and MIMEImage values tagged \

    with those cids.

    :param list[str]|None image_paths:

    :rtype: dict

    '''

    if not image_paths:
        return {}

    part_dict = {}

    for fpath in image_paths:
        fn = basename(fpath)

        image_cid = make_cid(fn)

        with open(fpath, "rb") as f:
            part = MIMEImage(f.read())

        # After the file is closed

        part.add_header('Content-Disposition', 'attachment; filename="%s"' % fn)

        part.add_header('Content-ID', image_cid)

        part_dict[image_cid] = part

    return part_dict


def image_file_dict_to_html(

        image_file_dict,

        fig_separator='<br><br>', key_as_caption=True,

        width_ovrd=None, height_ovrd=None, fig_subtext_dict=None

):
    '''

     :description: Uses image_file_dict keys (strings) and fig values to create attachable images with associated \

     cids used in the returned html block. Use this html to in-line the attached images.

     :param dict image_file_dict: dictionary of paths to image files. Key is used for caption if enabled.

     :param str fig_separator: vertical separator between figures.

     :param bool key_as_caption: Show dictionary key as caption above image

     :param int width_ovrd: integer pixel override to reserve page space (only specify height OR width)

     :param int height_ovrd: integer pixel override to reserve page space (only specify height OR width)

     :param None|dict fig_subtext_dict: a dict of (html) strings to put beneath each figure with keys matching fig_dict.

     :return: dictionary with 'html', block of html with images stacked vertically, \

     'img_list' as list of images to use as attachments.

     '''

    img_list = []

    html_list = []

    for k, v in image_file_dict.items():
        img_dict = load_image_files([v])

        cid = list(img_dict.keys())[0]

        img = img_dict[cid]

        img_list.append(img)

        caption = f'<div class="caption">{k}</div>' if key_as_caption else ''

        html_item = make_cid_html(cid, width_ovrd, height_ovrd)

        subtext = fig_subtext_dict.get(k, '') if fig_subtext_dict else ''

        html_list.append(caption + html_item + subtext)

    full_html = fig_separator.join(html_list)

    return {'html': full_html,

            'img_list': img_list}