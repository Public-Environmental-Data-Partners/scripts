import json
import lzma
import os.path
import re
import shutil
import tarfile
from io import BytesIO, StringIO
from tarfile import TarInfo
from typing import Optional, Any

import requests
from lxml import etree


'''
JS script:
function downloadimg(ele){

	var getfilename = ele.id
	var table = $('#chartTable').DataTable()
	table.column(0).visible(true);
	var mobile = "false";
	if (Array.from(getfilename)[0] == "y"){
		var tempfile = "d"+getfilename.substr(1);
		ele = document.getElementById(tempfile);
		mobile = "true";
	}

	
	
	var getid = ele.id
	var filename = getid.substr(1);

	var fileExt = ele.getAttribute("name");

	if (fileExt == "zip" || fileExt == "ZIP"){
		fileExt = ".zip";
	}
	else{
		fileExt = ".jpg";
	}

	if (mobile == "true"){
		table.column(0).visible(false);
	}
	
	window.location.href = "includes/downloadsingle.php?filename="+filename+"&fileExt="+fileExt;

}
'''

class MapItem:
    identifier: str
    image_link: str
    file_name: str
    # The extents in lat/lon coords. For example
    # -77.17495258187105 34.804882794671,
    # -75.2843047041253 34.80490291872108,
    # -75.28434948122984 37.08598083537176,
    # -77.17496992719673 37.08598325658462,
    # -77.17495258187105 34.804882794671
    extents: Optional[str]
    year_published: int
    edition: str
    chart_number: str
    scale: int

def download_jpgs(session: requests.Session, filename: str):
    #  https://historicalcharts.noaa.gov/jpgs/13309-02-2016.jpg
    download_link = f' https://historicalcharts.noaa.gov/jpgs/{filename}.jpg'
    local_link = f'historicalcharts.noaa.gov/jpgs/{filename}.jpg'
    if os.path.isfile(local_link):
        return download_link, local_link
    resp = session.get(download_link, stream=True)
    with open(local_link, 'wb') as f:
        resp.raw.decode_content = True
        shutil.copyfileobj(resp.raw, f)

    return download_link, local_link

def download_image(archive: tarfile.TarFile, session: requests.Session, link_id: str, file_type: str):
    filename = link_id[1:]
    file_ext = '.' + file_type.lower()
    download_link = f'https://historicalcharts.noaa.gov/includes/downloadsingle.php?filename={filename}&fileExt={file_ext}'
    # https://historicalcharts.noaa.gov/includes/downloadsingle.php?filename=0-0-0&fileExt=.jpg
    # https://historicalcharts.noaa.gov/includes/downloadsingle.php?filename=d0-0-0&fileExt=.jpg
    local_link = f'historicalcharts.noaa.gov/includes/downloadsingle.php!filename={filename}&fileExt={file_ext}'
    full_local_path = os.path.join(os.getcwd(), local_link.replace('/', os.path.sep))
    if os.path.isfile(full_local_path):
        print(f"ADDING LOCAL FILE {full_local_path}")
        archive.add(full_local_path, arcname=local_link)
        os.unlink(full_local_path)
        return download_link, local_link

    # Check if there's already a member in the archive.
    try:
        archive.getmember(local_link)
        return download_link, local_link
    except KeyError:
        pass

    print(f"Downloading image {link_id}")
    resp = session.get(download_link)
    resp.raw.decode_content = True
    bytebuf = BytesIO(resp.content)
    tinfo = TarInfo(local_link)
    tinfo.size = bytebuf.getbuffer().nbytes
    archive.addfile(tinfo, bytebuf)

    return download_link, local_link

def get_yr(archive: tarfile.TarFile, out_dict, year, inc):
    retrieve_search(archive, out_dict, f'https://historicalcharts.noaa.gov/includes/imageDBDT.php?title=&chart=&yearMin={year}&yearMax={year+inc}&singleYear=&type=Any%20Type&state=Any&scale=All%20Scales&latitude=&longitude=&js=no')
def iter_charts_by_year(first_year=1607, last_year=2040, inc=20):
    # We actually do have a few '0' years we don't wanna miss, so we grab a general set first
    if first_year <= 1607:
        out_dict = {}
        charts_range = 'Charts-0000-0000'
        charts_archive = f'{charts_range}.tar'
        charts_blob = f'{charts_range}.json'
        if os.path.isfile(charts_blob):
            print('Found existing charts file: ' + charts_range)
            if not os.path.isfile(charts_archive + '.xz') and not os.path.isfile(charts_archive + '.gz'):
                migrate_blob(charts_blob, charts_archive)
        else:
            with tarfile.open('Charts-0000-0000.tar', mode='a') as archive:
                retrieve_search(archive, out_dict, f'https://historicalcharts.noaa.gov/includes/imageDBDT.php?title=&chart=&yearMin=&yearMax=&singleYear=0000&type=Any%20Type&state=Any&scale=All%20Scales&latitude=&longitude=&js=no')
                dict_str = json.dumps(out_dict)
                buf = StringIO()
                buf.write(dict_str)
                buf.seek(0)
                tinfo = TarInfo(name=charts_blob)
                tinfo.size = len(dict_str)
                archive.addfile(tarinfo=tinfo, fileobj=buf)
            xz_file(charts_archive)

    # retrieve_search('https://historicalcharts.noaa.gov/includes/imageDBDT.php?title=&chart=&yearMin=&yearMax=&singleYear=&type=Any%20Type&state=Any&scale=All%20Scales&latitude=&longitude=&js=no')
    for year in range(first_year, last_year, inc):
        print(f"Scanning year range {year} - {year+inc} ")
        out_dict = {}
        charts_range = f'Charts-{year}-{year+inc}'
        charts_archive = f'{charts_range}.tar'
        charts_blob = f'{charts_range}.json'
        if os.path.isfile(charts_blob):
            print('Found existing charts file: ' + charts_blob)
            if not os.path.isfile(charts_archive + '.xz') and not os.path.isfile(charts_archive + '.gz'):
                migrate_blob(charts_blob, charts_archive)
            continue
        # Dear future reviewer. Originally these were all stored in one giant out_dict json blob.
        # I know sorting them out like this will make it a pain in the butt to re-assemble them, but unfortunately storing
        # this blob in memory was getting insane and if a single request failed I had to re-crawl everything.
        # Sorry!
        # - Archiver
        with tarfile.open(charts_archive, 'a') as archive:
            get_yr(archive, out_dict, year, inc)
            dict_str = json.dumps(out_dict)
            buf = StringIO()
            buf.write(dict_str)
            buf.seek(0)
            tinfo = TarInfo(name=charts_blob)
            tinfo.size = len(dict_str)
            archive.addfile(tarinfo=tinfo, fileobj=buf)
        xz_file(charts_archive)

def xz_file(filename):
    zip_name = filename + '.xz'
    print(f"Compressing {filename} to {zip_name}")
    if os.path.isfile(zip_name) or os.path.isfile(filename + '.gz'):
        return
    with lzma.open(zip_name, mode='w') as zipped:
        with open(filename, 'rb') as archive_raw:
            shutil.copyfileobj(archive_raw, zipped)
    print("Nuking uncompressed file " + filename)
    os.unlink(filename)

def migrate_blob(blob_filename: str, archive_filename: str):

    # Migrates from old blob format to new archive format
    session = requests.Session()
    with open(blob_filename, 'r') as f:
        entries: dict[str, Any] = json.load(f)
    with tarfile.open(archive_filename, 'a') as archive:
        for entry_key in entries.keys():
            download_image(archive, session, entry_key, "jpg")
    xz_file(archive_filename)


def retrieve_search(archive, out_dict, url):
    # https://historicalcharts.noaa.gov/search.php?search=search_col

    session = requests.Session()
    response = session.get(url)
    resp_body = response.text
    # Should be ZIP or JPG
    # This was from before we parsed HTML, but just to give you an idea of the kind of element we're looking for:
    # id_regexp = re.compile(r"<span id=([^\s]*?) name=sid class='[^']*' onclick='downloadimg\(this\)'>(JPG|ZIP)<\/span>")
    html_dat = etree.HTML(resp_body)
    table = html_dat.find(".//*[@id='chartTable']")
    headers = [h.text for h in table[0][0]]
    tlen = len(table[1])
    if tlen >= 5000:
        print("LIMIT DETECTED! WE MAY MISS SOMETHING")
        exit(1)
    for idx, row in enumerate(iter(table[1])):
        print(f'{idx}/{tlen} ({idx/tlen*100:.2f}%)')
        row_dat = [col.text if col.text else col[0] if len(col) > 0 else None for col in row]
        link_id = row_dat[0].attrib['id']
        if link_id in out_dict:
            print('Found existing entry for link_id: ' + link_id)
            continue
        link_type = row_dat[0].text
        row_dat[0] = row_dat[0].attrib['id']
        # preview row
        row_dat[1] = row_dat[1].attrib['id']
        print('Downloading ' + link_id + '.' + link_type)
        download_link, local_link = download_image(archive, session, link_id, link_type)

        out_dat = {}
        for idx, keys in enumerate(headers):
            out_dat[headers[idx].strip()] = row_dat[idx]
        out_dat['Title'] = out_dat['Title'].text
        if out_dat.get('Extent') is not None:
            # We get the first child of extent, which is an img tag. This img tag has an onclick method that includes the coordinates
            child = out_dat['Extent'][0]
            onclick = child.attrib['onclick']
            coord_pairs = re.match(r"dispGeom\('(.*?)'\);", onclick).group(1)
            # We COULD parse this to a float, but I am worried about precision and don't want to screw up the data, so we will keep it as a string. Sorry.
            out_dat['Extent'] = [pair.split(' ') for pair in coord_pairs.split(',')]


        out_dict[link_id] = out_dat


"""
This is the JS function:
function prevPubChart(id){
	var modal = document.getElementById('pubchart');
	modal.style.display = "block";
	
	//var iframe = document.getElementById('contentIframe');
	//iframe.src = id;
	//var pubele = "subBody"+ele;
	var tableDisplay = document.getElementById("chartTable");


	$.get("includes/pubCharts.php", {id:id}, function(data,status){
		//alert("Data: " + data + "\nStatus: " + status);
		// Log a message to the console
		tableDisplay.innerHTML = data;
		minispinneroff();
	});
}

"""
def get_related_charts(sess: requests.Session, publication_id: str):
    resp = sess.get('https://historicalcharts.noaa.gov/includes/pubCharts.php?id=' + publication_id)
    row_texts = resp.text
    html_dat = etree.HTML(row_texts)

    for tbody in html_dat:
        header = [h.text for h in tbody[0]]
        row = tbody[1]
        # First link is a download link like
        # /image.php?filename=13309-02-2016
        # Which becomes https://historicalcharts.noaa.gov/jpgs/13309-02-2016.jpg
        jpg_url = row[0][0].attrib['href']
        # download_jpgs
        filename = jpg_url.split('/')[-1]
        download_link, local_link = download_jpgs(sess, filename)
        row[0][0] = download_link

        data = dict(zip(header, row))
        yield data


def download_pdf(sess: requests.Session, url: str):
    plen = len('https://')
    local_path = url[plen:]
    if os.exists(local_path):
         return

    resp = sess.get(url, stream=True)
    with open(local_path, 'wb') as f:
        resp.raw.decode_content = True
        shutil.copyfileobj(resp.raw, f)


def download_publications():
    sess = requests.Session()
    pub_url = 'https://historicalcharts.noaa.gov/includes/pubDBDT.php?title=&yearMin=&yearMax=&singleYear=&type=Any%20Type&keyword=&docnum='
    response = sess.get(pub_url)
    resp_body = response.text
    html_dat = etree.HTML(resp_body)
    table = html_dat.find(".//*[@id='pubTable']")
    headers = [h.text for h in table[0][0]]
    if len(table[1]) >= 5000:
        print("LIMIT DETECTED! WE MAY MISS SOMETHING")
        exit(1)

    out_pub_dict = {}
    # Iterate all publications
    for row in iter(table[1]):
        row_dat = [col.text if col.text else col[0] if len(col) > 0 else None for col in row]
        # This is the link to the PDF
        pdf_link = row_dat[0].attrib['href']
        if pdf_link in out_pub_dict:
            print('Found existing entry for publication href: ' + pdf_link)
            continue

        download_pdf(sess, pdf_link)
        row_dat[0] = pdf_link
        # Get related charts
        related_chart_btn = row_dat[-1]
        if related_chart_btn:
            row_dat[-1] = [chart for chart in get_related_charts(ses, related_chart_btn)]

        out_dat = {}
        for idx, keys in enumerate(headers):
            out_dat[headers[idx]] = row_dat[idx]
        out_pub_dict[pdf_link] = out_dat
    return out_pub_dict


def main():
    # iter_charts_by_year(first_year=1607, last_year=1907, inc=20)
    # iter_charts_by_year(first_year=1907, last_year=1927, inc=20)
    # iter_charts_by_year(first_year=1927, last_year=1967, inc=10)
    iter_charts_by_year(first_year=1967, last_year=2050, inc=1)

    # data = download_publications()
    # with open('publications.json', 'w') as f:
    #     json.dump(data, f)


if __name__ == '__main__':
    main()