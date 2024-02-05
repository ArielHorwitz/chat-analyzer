
def write_html(figures, file, title='Plots', light=False):
    first_div = figures[0].to_html(full_html=False, include_plotlyjs='cdn' if light else True)
    divs = [first_div]+[fig.to_html(full_html=False, include_plotlyjs=False) for fig in figures[1:]]
    body = '\n'.join(get_part(_) for _ in divs)
    with open(file, 'w') as f:
        f.write(get_body(title, body))


def write_css(output_dir):
    with open(output_dir / 'layout.css', 'w') as f:
        f.write(CSS)


def get_body(title, body):
    body_title = BODY1.replace('<title></title>', f'<title>{title}</title>')
    return '\n'.join([body_title, body, BODY2])


def get_part(div):
    return '\n'.join([PART1, div, PART2])


BODY1 = """
<html>
<head>
<link rel="stylesheet" type="text/css" href="layout.css"/>
</head>

<title></title>

<body>

<div class="main-container">

"""

PART1 = """
<div class="figure-box">
"""

PART2 = """
</div>
"""

BODY2 = """

</div>

</body>
</html>
"""


CSS = """
body {background-color:#205050;}


.main-container {
  width: 94%;
  margin-left:auto;margin-right:auto;
  margin-bottom:20px;
}

.figure-box {
  width:98%;
  padding:1%;
  float:left;
  margin-left:auto;
  margin-right:1%;
  margin-bottom:1%;
  background-color:#40b080;
  border-radius:10px;
  box-shadow: 2px 2px 15px -5px;
}

@media screen and (min-width: 1501px) {

.figure-box {
  width:47%;
 }

}
"""
