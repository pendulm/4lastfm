# coding: utf-8
from users import update_targets
from tracks import update_top_track
from listened_history import update_targets_history
from relation import scheduling_scrape
import os

def update_all(week):
    directory = "data/week_%d" % week
    if not os.path.exists(directory):
        os.makedirs(directory)
    update_targets(week)
    update_top_track(week)
    update_targets_history(week)
    scheduling_scrape(week)


if __name__ == "__main__":
    week = 27
    update_all(week)

