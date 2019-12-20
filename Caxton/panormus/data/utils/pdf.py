import base64

from io import BytesIO

import os

import sys

from jinja2 import Environment, FileSystemLoader

from matplotlib.backends.backend_pdf import PdfPages

import pdfkit

from PyPDF2 import PdfFileMerger, PdfFileReader

from xhtml2pdf import pisa

from panormus.config.settings import WKHTML_BIN_DIR

sys.path.append(WKHTML_BIN_DIR)


def merge_pdfs(pdf_merge, pdf_path_1, pdf_path_2):
    """

    :description: combine two pdfs: pdf_path_1 and pdf_path_2 to pdf_merge

    :param str pdf_merge: output file name

    :param str pdf_path_1: path for the first pdf to be merged

    :param str pdf_path_2: path for the second pdf to be merged

    :return: None

    """

    merger = PdfFileMerger()

    merger.append(PdfFileReader(open(pdf_path_1, 'rb')))

    merger.append(PdfFileReader(open(pdf_path_2, 'rb')))

    merger.write(pdf_merge)


def create_pdf_from_figures(figures, figures_per_page=1, output_pdf_file=None):
    """

    :param figures: iterable of matplotlib figures

    :param int figures_per_page: figures per page

    :param output_pdf_file: full path to save pdf file

    :return: None

    """

    pp = PdfPages(output_pdf_file)

    for n, fig in enumerate(figures):

        if figures_per_page != 1:

            # TODO: implement multiple figures per page logic

            raise NotImplementedError('Multiple figures per page not yet implemented')

        else:

            fig.text(4.25 / 8.5, 0.5 / 11., str(n + 1), ha='center', fontsize=12)

            pp.savefig(fig)

    pp.close()


def convert_html_file_to_pdf(html_path, output_path):
    """

    :description: save html as pdf file

    :param str html_path: location to read html

    :param str output_path: full path with filename to save pdf

    :return: error messages

    :rtype: str

    """

    pisa.showLogging()

    with open(html_path, 'r') as f:
        html = f.read()

    with open(output_path, 'w+b') as f:
        pisaStatus = pisa.CreatePDF(html, dest=f)

        return pisaStatus.err


def convert_html_file_to_pdf_pdfkit(html_path, output_path, pdfkit_options=None):
    # to pdf

    if not pdfkit_options:
        pdfkit_options = {

            'footer-right': '[page] of [topage]', 'orientation': 'landscape'

        }

    config = pdfkit.configuration(wkhtmltopdf=os.path.join(WKHTML_BIN_DIR, 'wkhtmltopdf.exe'))

    pdfkit.from_file(html_path, output_path, configuration=config, options=pdfkit_options)


def convert_fig_to_html(fig):
    """

    :description: Convert Matplotlib figure 'fig' into a <img> tag for HTML use using base64 encoding.

    :param figure fig: a figure object to be converted

    :return: html

    :rtype: str

    """

    tmpfile = BytesIO()

    fig.savefig(tmpfile, format='png')

    encoded = base64.b64encode(tmpfile.getvalue())

    html = '<img src="data:image/png;base64,{}" class="img-fluid">'.format(encoded.decode('utf-8'))

    return html


def convert_img_to_html(img):
    """

    :description: Convert image object 'img' into a <img> tag for HTML use using base64 encoding.

    :param image img: an image object to be converted

    :return: None

    """

    tmpfile = BytesIO()

    img.save(tmpfile, format='PNG')

    tmpfile.seek(0)

    data_uri = base64.b64encode(tmpfile.read()).decode('ascii')

    html = '<img src="data:image/png;base64,{}" class="img-fluid">'.format(data_uri)

    return html


def render_jinja_template(

        template_file, **render_kwargs

):
    """

    :param str template_file: full path to template file

    :param render_kwargs: any extra rendering kwargs for jinja template

    :rtype: str

    :return: completed template html

    """

    env = Environment(loader=FileSystemLoader(os.path.dirname(template_file)))

    template = env.get_template(os.path.basename(template_file))

    output_from_parsed_template = template.render(**render_kwargs)

    return output_from_parsed_template