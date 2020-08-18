#!/usr/bin/env python2
# -*- coding: utf-8 -*-

import csv
from collections import namedtuple
import os
import shutil
import random
from osgeo.osr import SpatialReference

from core.model.MerraRequest import MerraRequest
from core.model.GeospatialImageFile import GeospatialImageFile

from maxent.model.MaxEntRequest import MaxEntRequest
from maxent.model.ObservationFile import ObservationFile

# Trial must be declared here, so Pickle can find the definition.
Trial = namedtuple('Trial', ['directory', 'obsFile', 'images'])


# -----------------------------------------------------------------------------
# class MmxRequest
# -----------------------------------------------------------------------------
class MmxRequest(object):
    
    # -------------------------------------------------------------------------
    # __init__
    # -------------------------------------------------------------------------
    def __init__(self, observationFile, dateRange, collection, variables,
                 operation, numTrials, outputDirectory):

        if not os.path.exists(outputDirectory):
            raise RuntimeError(str(outputDirectory)) + ' does not exist.'

        if not os.path.isdir(outputDirectory):
            raise RuntimeError(str(outputDirectory) + ' must be a directory.')

        # ---
        # The top-level directory structure.
        # - outputDirectory
        #   - merra: raw merra files from requestMerra()
        #   - asc: merra files prepared for maxent.jar
        #   - trials: contains trial-n directories, one for each trial.
        # ---
        self._outputDirectory = outputDirectory
        self._ascDir = os.path.join(self._outputDirectory, 'asc')
        self._merraDir = os.path.join(self._outputDirectory, 'merra')
        self._trialsDir = os.path.join(self._outputDirectory, 'trials')
        self._numTrials = numTrials
        self._observationFile = observationFile
        self._dateRange = dateRange
        self._collection = collection
        self._variables = variables
        self._operation = operation

        if not os.path.exists(self._ascDir):
            os.mkdir(self._ascDir)
            
        if not os.path.exists(self._merraDir):
            os.mkdir(self._merraDir)
            
        if not os.path.exists(self._trialsDir):
            os.mkdir(self._trialsDir)

    # -------------------------------------------------------------------------
    # compileContributions
    # -------------------------------------------------------------------------
    def _compileContributions(self, trials):

        # ---
        # Loop through the trials creating a dictionary like:
        # {predictor: [contribution, contribution, ...],
        #  predictor: [contribution, contribution, ...]} 
        # ---
        contributions = {}
        CONTRIB_KWD = 'permutation'

        for trial in trials:

            resultsFile = os.path.join(trial.directory,
                                       'maxentResults.csv')

            results = csv.reader(open(resultsFile))

            try:
                header = results.__next__()

            except:
                raise RuntimeError('Error reading ' + str(resultsFile))

            for row in results:

                rowDict = dict(zip(header, row))

                for key in rowDict.keys():

                    if CONTRIB_KWD in key:

                        newKey = key.split(CONTRIB_KWD)[0].strip()

                        if newKey not in contributions:
                            contributions[newKey] = []

                        contributions[newKey].append(float(rowDict[key]))

            return contributions

    # -------------------------------------------------------------------------
    # _getMerra
    # -------------------------------------------------------------------------
    def _getMerra(self):

        # ---
        # Copy clipped merra variable files to the merra directory.  
        # MerraRequest does not process files already in this merra directory.
        # ---
        merraFiles = MerraRequest.run(self._observationFile.envelope(),
                                      self._dateRange,
                                      MerraRequest.MONTHLY,
                                      [self._collection],
                                      self._variables,
                                      [self._operation],
                                      self._merraDir)
                                      
        # Instantiate GeospatialImageFiles from the paths.
        merraGifs = [GeospatialImageFile(f) for f in merraFiles]
        
        return merraGifs

    # -------------------------------------------------------------------------
    # getTopTen
    # -------------------------------------------------------------------------
    def getTopTen(self, trials):

        # Get the contributions of each predictor over all trials.
        contributions = self._compileContributions(trials)

        # Compute the average contribution of each predictor over all trials.
        averages = {}

        for key in contributions.keys():
            samples = contributions[key]
            averages[key] = float(sum(samples) / max(len(samples), 1))

        # ---
        # Sort the averages to get the most significant contributors at the
        # top of the list.
        # ---
        sortedAvgs = sorted(averages.items(),
                            key=lambda x: x[1],
                            reverse=True)[:10]

        topTen = []

        for k, v in sortedAvgs:
            
            pred = os.path.join(self._merraDir, k + '.nc')
            topTen.append(GeospatialImageFile(pred))

        return topTen

    # -------------------------------------------------------------------------
    # getTrialImageIndexes
    #
    # This method returns a list of lists.  Each inner list contains ten
    # randomly-chosen indexes into the set of MERRA input images.
    #
    # [[1, 3, 8, 4, ...], [31, 4, 99, ...], ...]
    # -------------------------------------------------------------------------
    def getTrialImagesIndexes(self, images):

        # Generate lists of random indexes in the files.
        indexesInEachTrial = []
        PREDICTORS_PER_TRIAL = 10
        
        if len(images) <= PREDICTORS_PER_TRIAL:
            
            msg = 'There are ' + \
                  str(len(images)) + \
                  ' images and ' + \
                  str(PREDICTORS_PER_TRIAL) + \
                  ' predictors required for each trial.  ' + \
                  'This is insufficient to generate random sets of ' + \
                  'predictors for the trials.  Consider broadening the ' + \
                  'image request.'
                  
            raise RuntimeError(msg)

        for i in range(1, int(self._numTrials) + 1):
            indexesInEachTrial.append(random.sample(range(0, len(images) - 1),
                                                    PREDICTORS_PER_TRIAL))

        return indexesInEachTrial

    # -------------------------------------------------------------------------
    # prepareImages
    #
    # Prepare the images once, to be copied to trial directories.  Consider
    # implementing this to run in parallel in MmxRequestCelery.
    # -------------------------------------------------------------------------
    def _prepareImages(self, merraGifs):
                
        # Perform the MaxEnt image preparation on this master set of images.
        mer = MaxEntRequest(self._observationFile, merraGifs, self._ascDir)
        preparedImageFiles = mer.prepareImages()
        preparedGifs = [GeospatialImageFile(f) for f in preparedImageFiles]        
        return preparedGifs
        
    # -------------------------------------------------------------------------
    # prepareOneTrial
    # -------------------------------------------------------------------------
    def prepareOneTrial(self, images, trialImageIndexes, trialNum):

        # Create a directory for this trial.
        TRIAL_NAME = 'trial-' + str(trialNum)
        TRIAL_DIR = os.path.join(self._trialsDir, TRIAL_NAME)

        if not os.path.exists(TRIAL_DIR):
            os.mkdir(TRIAL_DIR)

        # Copy the samples file to the trial.
        obsBaseName = os.path.basename(self._observationFile._filePath)
        trialObsPath = os.path.join(TRIAL_DIR, obsBaseName)
        shutil.copyfile(self._observationFile.fileName(), trialObsPath)

        trialObs = ObservationFile(trialObsPath,
                                   self._observationFile.species())

        # Get this trial's predictors.
        trialPredictors = [images[i] for i in trialImageIndexes]

        # Copy the images to the trial.
        trialAscDir = os.path.join(TRIAL_DIR, 'asc')
        
        if not os.path.exists(trialAscDir):
            os.mkdir(trialAscDir)
            
        for ascGif in trialPredictors:
            
            trialAscGif = os.path.join(trialAscDir,
                                       os.path.basename(ascGif.fileName()))
                                       
            if not os.path.exists(trialAscGif):
                shutil.copyfile(ascGif.fileName(), trialAscGif)

        # Build the Trial structure to use later.
        trial = Trial(directory=TRIAL_DIR,
                      images=trialPredictors,
                      obsFile=trialObs)

        return trial

    # -------------------------------------------------------------------------
    # run
    # -------------------------------------------------------------------------
    def run(self):
        
        # ---
        # Get MERRA images.  This returns GeospatialImageFile objects of the
        # .nc files.
        # ---
        merraImages = self._getMerra()
        
        # Prepare the images for MaxEnt.
        images = self._prepareImages(merraImages)

        # Get the random lists of indexes into Images for each trial.
        listOfIndexesInEachTrial = self.getTrialImagesIndexes(images)

        # ---
        # Prepare the trials.
        #
        # - outputDirectory
        #   - trials
        #     - trial-1
        #       - observation file
        #       - asc
        #         - asc image 1
        #         - asc image 2
        #         ...
        #     - trial-2
        #     ...
        # ---
        trialNum = 0
        trials = []

        for trialImageIndexes in listOfIndexesInEachTrial:
            
            trials.append(self.prepareOneTrial(images,
                                               trialImageIndexes,
                                               trialNum + 1))
            trialNum += 1

        # Run the trials.
        self.runTrials(trials)

        # Compile trial statistics and select the top-ten predictors.
        topTen = self.getTopTen(trials)

        # Run the final model.
        final = self.prepareOneTrial(topTen, range(0, len(topTen) - 1),
                                     'final')
                                     
        finalMer = MaxEntRequest(final.obsFile, final.images, final.directory)
        finalMer.run()

    # -------------------------------------------------------------------------
    # runTrials
    # -------------------------------------------------------------------------
    def runTrials(self, trials):
        
        numTrials = len(trials)
        curTrialNum = 0
        
        for trial in trials:
            
            curTrialNum += 1
            print('Running trial', curTrialNum, 'of', numTrials)
            mer = MaxEntRequest(trial.obsFile, trial.images, trial.directory)
            mer.run()
