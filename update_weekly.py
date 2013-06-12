# coding: utf-8
from users import update_targets
from tracks import update_top_tracks
from listened_history import update_targets_history

def update_all(week):
    update_targets(week)
    update_top_tracks(week)
    update_targets_history(week)


if __name__ == "__main__":
    week = 24
    update_all(24)

