# Builds docs/updating-the-data.pdf, the step-by-step guide for adding a year of CalPIP data.
#   pip install reportlab && python docs/guide/make_guide.py
# Screenshots in img/ are from CalPIP and GitHub. Fonts are macOS system Arial; change FONT_DIR elsewhere.
from pathlib import Path
from reportlab.lib import colors
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.units import inch
from reportlab.graphics.shapes import Drawing, Rect, String, Line, Polygon
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.platypus import (Image, Indenter, KeepTogether, ListFlowable, ListItem, Paragraph, SimpleDocTemplate,
                                Spacer, Table, TableStyle)

HERE = Path(__file__).parent
OUT = HERE.parent / "updating-the-data.pdf"
FONT_DIR = Path("/System/Library/Fonts/Supplemental")
for name, file in [("Arial", "Arial.ttf"), ("Arial-Bold", "Arial Bold.ttf"), ("Arial-Italic", "Arial Italic.ttf"),
                   ("Mono", "Courier New.ttf")]:
  pdfmetrics.registerFont(TTFont(name, FONT_DIR / file))
pdfmetrics.registerFontFamily("Arial", normal="Arial", bold="Arial-Bold", italic="Arial-Italic", boldItalic="Arial-Bold")

ACCENT = colors.HexColor("#1f5f8b")
TINT = colors.HexColor("#eef4f8")
WARN_TINT = colors.HexColor("#fdf6e3")
WIDTH = 6.5 * inch

body = ParagraphStyle("body", fontName="Arial", fontSize=10.5, leading=15, spaceAfter=6)
small = ParagraphStyle("small", parent=body, fontSize=8.5, leading=11, textColor=colors.HexColor("#555555"))
h1 = ParagraphStyle("h1", fontName="Arial", fontSize=22, leading=27, spaceAfter=8)
h2 = ParagraphStyle("h2", fontName="Arial", fontSize=16, leading=20, spaceBefore=14, spaceAfter=6, textColor=ACCENT)
h3 = ParagraphStyle("h3", fontName="Arial-Bold", fontSize=11, leading=15, spaceBefore=8, spaceAfter=4)
cell = ParagraphStyle("cell", parent=body, fontSize=9.5, leading=13, spaceAfter=0)


def P(text, style=body):
  return Paragraph(text, style)


def code(text):
  return f'<font name="Mono">{text}</font>'


def steps(items, start=1):
  return ListFlowable([ListItem(P(i), leftIndent=16, value=n) for n, i in enumerate(items, start)],
                      bulletType="1", start=start, leftIndent=16, bulletFontName="Arial")


def bullets(items):
  return ListFlowable([ListItem(P(i), leftIndent=12) for i in items], bulletType="bullet", start="•",
                      leftIndent=12, bulletFontName="Arial")


def box(flowables, tint=TINT, edge=ACCENT):
  t = Table([[flowables]], colWidths=[WIDTH])
  t.setStyle(TableStyle([("BACKGROUND", (0, 0), (-1, -1), tint), ("LINEBEFORE", (0, 0), (0, -1), 3, edge),
                         ("LEFTPADDING", (0, 0), (-1, -1), 12), ("RIGHTPADDING", (0, 0), (-1, -1), 12),
                         ("TOPPADDING", (0, 0), (-1, -1), 8), ("BOTTOMPADDING", (0, 0), (-1, -1), 4)]))
  return t


def shot(file, width=4.8 * inch):
  img = Image(str(HERE / "img" / file))
  img.drawWidth, img.drawHeight = width, width * img.imageHeight / img.imageWidth
  return img


def flow_diagram():
  d = Drawing(WIDTH, 78)
  labels = [("1  CalPIP", "Request a year,", "download the zip"), ("2  Cloudflare R2", "Upload the zip to", "raw/calpip/"),
            ("3  Build data", "Run it on GitHub,", "check the preview"), ("4  Publish data", "Run it on GitHub;", "the site updates")]
  w, gap = 100, (WIDTH - 4 * 100) / 3
  for i, (title, l1, l2) in enumerate(labels):
    x = i * (w + gap)
    d.add(Rect(x, 4, w, 70, rx=6, ry=6, fillColor=TINT if i < 3 else ACCENT, strokeColor=ACCENT))
    ink = colors.white if i == 3 else colors.black
    d.add(String(x + w / 2, 52, title, fontName="Arial-Bold", fontSize=10, textAnchor="middle", fillColor=ink))
    d.add(String(x + w / 2, 33, l1, fontName="Arial", fontSize=8.5, textAnchor="middle", fillColor=ink))
    d.add(String(x + w / 2, 21, l2, fontName="Arial", fontSize=8.5, textAnchor="middle", fillColor=ink))
    if i < 3:
      ax = x + w + 4
      d.add(Line(ax, 39, ax + gap - 12, 39, strokeColor=ACCENT, strokeWidth=1.5))
      d.add(Polygon([ax + gap - 12, 43, ax + gap - 6, 39, ax + gap - 12, 35], fillColor=ACCENT, strokeColor=ACCENT))
  return d


def page_number(canvas, doc):
  canvas.setFont("Arial", 8)
  canvas.setFillColor(colors.HexColor("#888888"))
  canvas.drawRightString(letter[0] - inch, 0.6 * inch, f"Updating the People and Pesticide Explorer data · page {doc.page}")


BUILD_URL = "https://github.com/open-spatial-lab/cpr/actions/workflows/build-data.yml"
PUBLISH_URL = "https://github.com/open-spatial-lab/cpr/actions/workflows/publish-data.yml"


def link(url, text=None):
  return f'<link href="{url}" color="#1f5f8b"><u>{text or url}</u></link>'


story = [
  P("People and Pesticide Explorer:<br/>Updating the Data", h1),
  P("Prepared by: Dylan Halpern, UChicago DSI<br/>Prepared for: Andrew Olsen, PAN<br/>Last updated: October 8, 2026", small),
  Spacer(1, 6),
  P("This guide replaces the March 2025 version. The explorer's data moved from Amazon Web Services (AWS) to "
    "Cloudflare R2 and GitHub Actions. Requesting data from CalPIP hasn't changed."),
  P("Adding a year of pesticide use data takes four steps:"),
  steps([
    "Request the year from CalPIP and download the zip file. This takes about 20 minutes, mostly waiting for an email.",
    "Upload the zip to the project's storage on Cloudflare R2.",
    "Run the <b>Build data</b> job on GitHub and check the results. This takes about 10 minutes.",
    "Run the <b>Publish data</b> job to put the new data on the live site.",
  ]),
  Spacer(1, 4),
  flow_diagram(),
  Spacer(1, 4),
  P("Nothing changes on the live site until step 4, and step 4 can be undone at any time."),
  box([
    P("<b>Before you start</b>", h3),
    bullets([
      "A Cloudflare login with access to the project's R2 bucket.",
      "A GitHub account with permission to run Actions in the <b>open-spatial-lab/cpr</b> repository.",
      "Dylan can set up both. Fill in these project details:",
    ]),
    P("R2 bucket name: ______________________________<br/>Explorer website: ______________________________"),
  ]),
  P("Key terms", h3),
  bullets([
    "<b>CalPIP</b>: the website of the California Department of Pesticide Regulation (DPR) for downloading "
    "Pesticide Use Report (PUR) data.",
    "<b>Cloudflare R2</b>: online file storage, similar to Dropbox or Google Drive. The explorer's data lives here.",
    "<b>Bucket</b>: the top-level folder for a project in R2.",
    "<b>GitHub Actions</b>: buttons on GitHub that run scripts. <i>Build data</i> and <i>Publish data</i> are two of them.",
    "<b>Build</b>: a complete processed copy of the explorer's data, named by the date and time it was made, such as "
    f"{code('v20270115-0930')}. Builds are never changed or deleted, which is what makes undoing possible.",
    "<b>ZIP archive</b>: a compressed file, CalPIP's download format. Upload it as it is.",
  ]),

  P("1. Request data from CalPIP", h2),
  P("CalPIP (" + link("https://calpip.cdpr.ca.gov", "calpip.cdpr.ca.gov") + ") is DPR's portal for pesticide use data. "
    "The explorer needs the full record-level data for the whole state: one row per pesticide application, not a "
    "summary. CalPIP allows one year per request, so repeat these steps for each year you're adding."),
  KeepTogether([steps(["Open CalPIP and choose <b>PUR</b> as the data source. Click <b>Year</b> in the navigation "
                       "menu and select the year you want. Leave the location, site, product and chemical filters "
                       "empty."]), shot("calpip-year.jpg")]),
  Spacer(1, 8),
  KeepTogether([steps(["Click <b>Format Output</b> in the navigation menu. Make sure <b>Summarize the Data</b> is "
                       "<b>not</b> checked, and keep all output columns selected. The build checks for the columns "
                       "it needs and will say if any are missing."], start=2), shot("calpip-summarize.jpg")]),
  Spacer(1, 8),
  KeepTogether([steps(["Under <b>Output File Format/Type</b>, keep <b>Tab-delimited text (default)</b> checked and "
                       "<b>HTML table</b> unchecked. Click <b>Submit Query</b>."], start=3), shot("calpip-format.jpg")]),
  Spacer(1, 8),
  KeepTogether([steps(["Enter your email address and click <b>Send Query Now</b>."], start=4), shot("calpip-email.jpg")]),
  Spacer(1, 8),
  steps(["CalPIP emails you a download link, usually within about 20 minutes. Download the zip file. Don't unzip it, "
         "rename the file inside it, or edit it. The link expires after 7 days."], start=5),

  P("2. Upload the zip to Cloudflare R2", h2),
  P(f"The zip goes in the folder {code('raw/calpip/')} in the project's bucket. That folder holds one file per year: "
    f"zips that people have uploaded, and files named {code('calpip_&lt;year&gt;.parquet')} for older years. Don't delete "
    "or rename those; every build uses all of them."),
  steps([
    f"Log in at {link('https://dash.cloudflare.com', 'dash.cloudflare.com')} and choose the project's account.",
    "In the left sidebar, open <b>R2 Object Storage</b>, then click the project's bucket.",
    f"Open the {code('raw')} folder, then {code('calpip')}.",
    "Click <b>Upload</b> and choose the zip from your computer. Wait until the upload finishes before leaving the page.",
  ]),
  box([P("<b>Replacing a year.</b> DPR sometimes revises past data. To re-pull a year, request it from CalPIP again, "
         "upload the new zip, and delete the old zip for that year if there is one. If both are in the folder, the "
         "build uses the one uploaded most recently.")]),
  Spacer(1, 8),
  box([
    P("<b>Large files</b>", h3),
    P("A full year from CalPIP is usually around 120 MB zipped, which the dashboard handles. Uploading through a "
      "browser can fail for much larger files; Cloudflare's own command-line uploader stops at 315 MB. If the upload "
      "fails, or the file is over 300 MB, use <b>Cyberduck</b>, a free app for Mac and Windows that splits large "
      "uploads into pieces automatically:"),
    steps([
      f"Install Cyberduck from {link('https://cyberduck.io', 'cyberduck.io')}.",
      "Get an access key (once). In the Cloudflare dashboard, open <b>R2 Object Storage</b> and click "
      "<b>Manage API tokens</b>. Create a token with <b>Object Read &amp; Write</b> permission for the project's "
      "bucket only. Copy the <b>Access Key ID</b>, the <b>Secret Access Key</b>, and the S3 endpoint, which looks like "
      f"{code('https://&lt;account id&gt;.r2.cloudflarestorage.com')}. The secret is shown only once, so save it in a "
      "password manager. Dylan can also give you an upload key.",
      "In Cyberduck, click <b>Open Connection</b> and choose <b>Amazon S3</b> from the list. For <b>Server</b>, enter "
      "the endpoint without <i>https://</i>, then the Access Key ID and Secret Access Key. Click <b>Connect</b>.",
      f"Open the bucket, then {code('raw')} and {code('calpip')}, and drag the zip into the window.",
    ]),
  ], tint=WARN_TINT, edge=colors.HexColor("#c9a227")),

  P("3. Build the data", h2),
  steps([
    f"Open {link(BUILD_URL, 'the Build data page')} (in the repository, click <b>Actions</b>, then <b>Build data</b> "
    "in the list on the left).",
    "Click <b>Run workflow</b>, then the green <b>Run workflow</b> button.",
  ]),
  shot("run-workflow.png", width=2.2 * inch),
  steps([
    "Wait for the run to finish, usually 5 to 10 minutes. Refresh the page: a green check means it worked, a red X "
    "means it stopped (see Troubleshooting).",
    "Click the run to open its summary page and check three things:",
  ], start=3),
  Indenter(20), bullets([
    f"<b>The version name and year range</b>, such as {code('v20270115-0930')}, years 2017–2024. The year you added "
    "should be the last year.",
    "<b>The yearly totals table.</b> The year you added shows as <i>new year</i>. Years already on the site should show "
    "0.00% unless you re-pulled that year. A large change in an older year is a reason to stop and ask before "
    "publishing.",
    "<b>The preview link.</b> It opens the explorer with this build's data and a banner saying it's a preview. Pick the "
    "new year in the date range and try a map and a chart.",
  ]),
  Indenter(-20),
  P("Building doesn't change the live site. You can run Build data as many times as you like; the preview link always "
    "shows the most recent build."),

  P("4. Publish the data", h2),
  steps([
    f"Open {link(PUBLISH_URL, 'the Publish data page')} (<b>Actions</b>, then <b>Publish data</b>).",
    "Click <b>Run workflow</b>. Leave the <b>version</b> box empty to publish the most recent build, the one the "
    "preview link shows. Click the green <b>Run workflow</b> button.",
    "It finishes in under a minute. Reload the explorer and it shows the new data. The website itself doesn't need "
    "to be rebuilt.",
  ]),
  box([P("<b>Undoing a publish.</b> The Publish data summary page names the version it replaced. To go back, run "
         "Publish data again and type that version name into the <b>version</b> box. Every build is kept, so you can "
         "return to any of them.")]),

  P("Troubleshooting", h2),
]

trouble = [
  ("Build data shows a red X, and the error mentions <i>missing CalPIP columns</i>.",
   "The zip is a summary or is missing columns. Request the year again (section 1, steps 2 and 3), replace the zip, "
   "and run Build data again."),
  ("The year you added isn't in the totals table.",
   f"The zip isn't in {code('raw/calpip/')}, or it was unzipped before uploading. Check the folder and that the file "
   "name ends in .zip."),
  ("Build data fails with a different error.",
   "Send Dylan the link to the failed run. The live site isn't affected."),
  ("The CalPIP download link has expired.", "Submit the CalPIP query again."),
  ("The upload in the Cloudflare dashboard fails.", "Use Cyberduck (section 2, Large files)."),
  ("The site shows the wrong data after publishing.", "Run Publish data with the previous version (section 4, Undoing a publish)."),
]
table = Table([[P("<b>Problem</b>", cell), P("<b>What to do</b>", cell)]] + [[P(a, cell), P(b, cell)] for a, b in trouble],
              colWidths=[2.6 * inch, 3.9 * inch], repeatRows=1)
table.setStyle(TableStyle([("BACKGROUND", (0, 0), (-1, 0), TINT), ("LINEBELOW", (0, 0), (-1, -1), 0.5, colors.HexColor("#cccccc")),
                           ("VALIGN", (0, 0), (-1, -1), "TOP"), ("TOPPADDING", (0, 0), (-1, -1), 5),
                           ("BOTTOMPADDING", (0, 0), (-1, -1), 5)]))
story += [table, Spacer(1, 10),
          P("For maintainers: the pipeline code and setup are described in the repository's README.", small)]

SimpleDocTemplate(str(OUT), pagesize=letter, leftMargin=inch, rightMargin=inch, topMargin=inch, bottomMargin=inch,
                  title="Updating the People and Pesticide Explorer data", author="Dylan Halpern, UChicago DSI"
                  ).build(story, onFirstPage=page_number, onLaterPages=page_number)
print("wrote", OUT)
