import logclass

###############################################################################
#c="bq show 774982414180:my_dataset.logs >> schema.txt"
###############################################################################

object = logclass.LogClass()

object.glog('This is, our 31th log','logged with, our class')
object.glog('This is, our 32st log','logged with our class')
object.glog('This is, our 33nd log','logged with logclass class')

###############################################################################
