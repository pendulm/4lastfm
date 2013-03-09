import cPickle
f=open("for_track.pkl" , "r")
obj = cPickle.load(f)
tracks = obj['tracks']
