def create_html_table(dat_format, include_index=True, index_width=None, cell_width=None, borders='bottom', ulines=[]):
    """create simple html table for in email"""
    html = "<table style=\"font-family: Calibri; font-size: 10pt;\">"
    html += "<tr>"
    # html += "<tr >"
    # html = "<table><tr>"

    if borders == 'bottom':
        b_b = "  style=\"border-bottom:1px solid #000000\""
        b_r = ""
        b_rb = b_b
    elif borders == 'rightbottom':
        b_b = "  style=\"border-bottom:1px solid #000000\""
        b_r = "  style=\"border-right:1px solid #000000\""
        b_rb = "  style=\"border-right:1px solid #000000; border-bottom:1px solid #000000\""

    if include_index:
        html += "<th " + b_rb + "></th>"

    for c in dat_format.columns:
        html += "<th align=\"center\"  {}>{}</th>".format(b_b, c)
    html += "</tr>"

    for i in range(len(dat_format.index)):
        cell_opt = "align=\"center\""
        if cell_width:
            cell_opt += "  width=\"{}\"".format(cell_width)

        index_opt = "align=\"left\""
        # index_opt = "align=\"left\"" + b_r
        if index_width:
            index_opt += "  width=\"{}\"".format(index_width)

        if i in ulines:
            index_opt += b_rb
            cell_opt += b_b
        else:
            index_opt += b_r

        html += "<tr>"
        if include_index:
            html += "<th {}>{}</th>".format(index_opt, dat_format.index[i])
        for x in dat_format.iloc[i, :].values:
            html += "<td {}>{}</td>".format(cell_opt, x)
        html += "</tr>"

    html += "</table>"
    return html