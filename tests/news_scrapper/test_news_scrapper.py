from news_scanner.news_scrapper import target_news_scrapper as msns


def test_to_datetime():
    """ Ensures a datetime obj can be created from time_str. """
    time_str = "Nov 9, 2021 2:03 AM UTC"
    time_obj = msns._to_datetime_cst(time_str)
    print(time_obj.strftime('%b %d %Y %I:%M %p'))


def test_get_latest_link_index():
    """ Ensures latest viewed link index is found. """
    viewed_links = ["link1", "link2", "link3", "link4", "link5"]
    links = ["linka", "linkb", "link1", "link2"]
    latest_index = msns._get_latest_link_index(links, viewed_links)
    assert latest_index is 2
