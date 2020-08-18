import os
import shutil

from celery import group

from core.model.GeospatialImageFile import GeospatialImageFile

from maxent.model.MaxEntRequestCelery import MaxEntRequestCelery

from merramax.model.CeleryConfiguration import app
from merramax.model.MmxRequest import MmxRequest


# -----------------------------------------------------------------------------
# class MmxRequestCelery
# -----------------------------------------------------------------------------
class MmxRequestCelery(MmxRequest):

    # -------------------------------------------------------------------------
    # __init__
    # -------------------------------------------------------------------------
    def __init__(self, observationFile, dateRange, collection, variables,
                 operation, numTrials, outputDirectory, numProcs=5):

        # Initialize the base class.
        super(MmxRequestCelery, self).__init__(observationFile,
                                               dateRange,
                                               collection,
                                               variables,
                                               operation,
                                               numTrials,
                                               outputDirectory)

    # -------------------------------------------------------------------------
    # prepareImages
    #
    # Prepare the images once, to be copied to trial directories.  Consider
    # implementing this to run in parallel in MmxRequestCelery.
    # -------------------------------------------------------------------------
    def _prepareImages(self, merraGifs):

        print('In MmxRequestCelery.prepareImages ...')

        # Perform the MaxEnt image preparation on this master set of images.
        mer = MaxEntRequestCelery(self._observationFile,
                                  merraGifs,
                                  self._ascDir)

        preparedImageFiles = mer.prepareImages()
        preparedGifs = [GeospatialImageFile(f.get()) for f in preparedImageFiles]
        return preparedGifs

    # -------------------------------------------------------------------------
    # prepareOneTrial
    #
    # In addition to the normal trial preparation, we must copy maxent.jar to
    # each trial when running in parallel.  Maxent.jar writes preferences to
    # disk, and multiple occurences cause file locking errors.
    # -------------------------------------------------------------------------
    def prepareOneTrial(self, images, trialImageIndexes, trialNum):

        trial = MmxRequest.prepareOneTrial(self,
                                           images,
                                           trialImageIndexes,
                                           trialNum)

        shutil.copyfile(MaxEntRequestCelery.MAX_ENT_JAR,
                        trial.directory + '/maxent.jar')

        return trial

    # -------------------------------------------------------------------------
    # runTrials
    # -------------------------------------------------------------------------
    def runTrials(self, trials):

        print('In MmxRequestCelery.runTrials ...')

        wpi = group(MmxRequestCelery._runOneTrial.s(trial) \
                for trial in trials)

        asyncResults = wpi.apply_async()  # This initiates the processes.
        asyncResults.get()    # Waits for wpi to finish.

    # -------------------------------------------------------------------------
    # runOneTrial
    # -------------------------------------------------------------------------
    @staticmethod
    @app.task(serializer='pickle')
    def _runOneTrial(trial):

        print('In MmxRequestCelery._runOneTrial ...')
        mer = MaxEntRequestCelery(trial.obsFile, trial.images, trial.directory)
        mer.runMaxEntJar(os.path.join(trial.directory, 'maxent.jar'))
