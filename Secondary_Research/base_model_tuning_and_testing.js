/**** Start of imports. If edited, may not auto-convert in the playground. ****/
var table = ee.FeatureCollection("projects/evs-es2110/assets/base_input");
/***** End of imports. If edited, may not auto-convert in the playground. *****/
// =================================================================================
//  1. CONFIGURATION
// =================================================================================
var year = 2018;
var month = 12;

var pathRowList = [
    [133, 40], [133, 41], [134, 40], [134, 41], [134, 42],
    [135, 39], [135, 40], [135, 41], [135, 42],
    [136, 40], [136, 41], [136, 42], [137, 41]
];

// Define the hyperparameter to test
var numTreesList = [50, 100, 150, 200, 250, 300];


// =================================================================================
//  2. PREPARE LABELED POINTS (Parentheses Fixed)
// =================================================================================
var points = table.filter(ee.Filter.notNull(['lon', 'lat']))
    .map(function (feature) {
        var lon = ee.Number.parse(feature.get('lon'));
        var lat = ee.Number.parse(feature.get('lat'));

        // This nested 'If' block is now correctly formatted and balanced.
        var classNumber = ee.Algorithms.If(ee.Number(feature.get('Forest')).eq(1), 0,
            ee.Algorithms.If(ee.Number(feature.get('Tree Based AG')).eq(1), 1,
                ee.Algorithms.If(ee.Number(feature.get('Wet Paddy')).eq(1), 2,
                    ee.Algorithms.If(ee.Number(feature.get('Grassland')).eq(1), 3,
                        ee.Algorithms.If(ee.Number(feature.get('Urban')).eq(1), 4,
                            ee.Algorithms.If(ee.Number(feature.get('Shifting Cultivation')).eq(1), 5,
                                ee.Algorithms.If(ee.Number(feature.get('Non-Tree based agriculture')).eq(1), 6,
                                    ee.Algorithms.If(ee.Number(feature.get('Other')).eq(1), 7, 99)
                                ))))))); // <-- CORRECT NUMBER OF PARENTHESES

        return feature.setGeometry(ee.Geometry.Point([lon, lat]))
            .set('target_class', classNumber);
    });

var finalPoints = points.filter(ee.Filter.lt('target_class', 99));
print("Total valid labeled points:", finalPoints.size());


// =================================================================================
//  3. CREATE THE PREDICTOR IMAGE
// =================================================================================
function maskL8sr(image) {
    var qa = image.select('QA_PIXEL');
    var cloudMask = (1 << 1) | (1 << 3) | (1 << 4);
    var mask = qa.bitwiseAnd(cloudMask).eq(0);
    var opticalBands = image.select('SR_B.*').multiply(0.0000275).add(-0.2);
    return image.updateMask(mask)
        .addBands(opticalBands, null, true)
        .copyProperties(image, ['CLOUD_COVER']);
}

var addQualityBand = function (image) {
    var quality = ee.Image.constant(100)
        .subtract(ee.Number(image.get('CLOUD_COVER')))
        .toFloat();
    return image.addBands(quality.rename('quality_score'));
};

var filters = pathRowList.map(function (pathRow) { return ee.Filter.and(ee.Filter.eq('WRS_PATH', pathRow[0]), ee.Filter.eq('WRS_ROW', pathRow[1])); });
var pathRowFilters = ee.Filter.or.apply(null, filters);
var startDate = ee.Date.fromYMD(year, month, 1);
var endDate = startDate.advance(1, 'month');

var mosaic = ee.ImageCollection('LANDSAT/LC08/C02/T1_L2')
    .filter(pathRowFilters)
    .filterDate(startDate, endDate)
    .map(maskL8sr)
    .map(addQualityBand)
    .qualityMosaic('quality_score');

var landsatBands = { 'blue': 'SR_B2', 'green': 'SR_B3', 'red': 'SR_B4', 'nir': 'SR_B5', 'swir1': 'SR_B6', 'swir2': 'SR_B7' };
var newNames = Object.keys(landsatBands);
var currentNames = newNames.map(function (key) { return landsatBands[key]; });
var mosaicRenamed = mosaic.select(currentNames, newNames);

function getNDVI(image) { return image.normalizedDifference(['nir', 'red']).rename("ndvi"); }
function getNDWI(image) { return image.normalizedDifference(['nir', 'swir1']).rename("ndwi"); }
function getSAVI(image) { return image.expression('1.5 * (b("nir") - b("red")) / (0.5 + b("nir") + b("red"))').rename("savi"); }
function getPRI(image) { return image.expression('(b("blue") - b("green")) / (b("blue") + b("green"))').rename("pri"); }
function getCAI(image) { return image.expression('b("swir2") / b("swir1")').rename("cai"); }
function getEVI(image) { return image.expression('2.5 * ((b("nir") - b("red")) / (b("nir") + 6 * b("red") - 7.5 * b("blue") + 1))').rename("evi"); }
function getEVI2(image) { return image.expression('2.5 * (b("nir") - b("red")) / (b("nir") + (2.4 * b("red")) + 1)').rename("evi2"); }
function getHallCover(image) { return image.expression('(-b("red") * 0.017) - (b("nir") * 0.007) - (b("swir2") * 0.079) + 5.22').rename("hallcover"); }
function getHallHeigth(image) { return image.expression('(-b("red") * 0.039) - (b("nir") * 0.011) - (b("swir1") * 0.026) + 4.13').rename("hallheigth"); }
function getGCVI(image) { return image.expression('b("nir") / b("green") - 1').rename("gcvi"); }

var mosaicWithIndices = mosaicRenamed.addBands(getNDVI(mosaicRenamed)).addBands(getNDWI(mosaicRenamed)).addBands(getSAVI(mosaicRenamed)).addBands(getPRI(mosaicRenamed)).addBands(getCAI(mosaicRenamed)).addBands(getEVI(mosaicRenamed)).addBands(getEVI2(mosaicRenamed)).addBands(getHallCover(mosaicRenamed)).addBands(getHallHeigth(mosaicRenamed)).addBands(getGCVI(mosaicRenamed));

var allPredictors = ['blue', 'green', 'red', 'nir', 'swir1', 'swir2', 'ndvi', 'ndwi', 'savi', 'pri', 'cai', 'evi', 'evi2', 'hallcover', 'hallheigth', 'gcvi'];
var predictorImage = mosaicWithIndices.select(allPredictors);


// =================================================================================
//  4. PREPARE TRAINING & TESTING DATA
// =================================================================================
finalPoints = finalPoints.randomColumn('random'); // Add a random column
var trainingSet = finalPoints.filter(ee.Filter.lte('random', 0.8));
var testingSet = finalPoints.filter(ee.Filter.gt('random', 0.8));

var trainingData = predictorImage.sampleRegions({ collection: trainingSet, properties: ['target_class'], scale: 30 });
var testingData = predictorImage.sampleRegions({ collection: testingSet, properties: ['target_class'], scale: 30 });


// =================================================================================
//  5. HYPERPARAMETER TUNING LOOP
// =================================================================================
print('--- Starting Hyperparameter Tuning ---');

var results = numTreesList.map(function (n_trees) {
    var classifier = ee.Classifier.smileRandomForest(n_trees).train({
        features: trainingData,
        classProperty: 'target_class',
        inputProperties: allPredictors
    });

    var validation = testingData.classify(classifier);
    var errorMatrix = validation.errorMatrix('target_class', 'classification');
    var accuracy = errorMatrix.accuracy();

    print('--- Results for ' + n_trees + ' trees ---');
    print('Overall Accuracy:', accuracy);
    print('Confusion Matrix:', errorMatrix);

    return ee.Feature(null, { 'trees': n_trees, 'accuracy': accuracy });
});

// =================================================================================
//  6. TRAIN FINAL MODEL AND VISUALIZE
// =================================================================================
// Find the best number of trees from the results
var resultsFC = ee.FeatureCollection(results);
var bestNumberOfTrees = ee.Number(resultsFC.sort('accuracy', false).first().get('trees'));

print('--- Final Model ---');
print('Best Number of Trees based on accuracy:', bestNumberOfTrees);

// Train the final classifier with the best parameter
var finalClassifier = ee.Classifier.smileRandomForest(bestNumberOfTrees).train({
    features: trainingData,
    classProperty: 'target_class',
    inputProperties: allPredictors
});

// Classify the image with the final model
var finalClassifiedImage = predictorImage.classify(finalClassifier);

var palette = ['#006400', '#3cb371', '#7fffd4', '#adff2f', '#ff4500', '#a52a2a', '#ffd700', '#808080'];
var vizParams = { min: 0, max: 7, palette: palette };

Map.centerObject(finalPoints, 10);
Map.addLayer(finalClassifiedImage, vizParams, 'Final Land Cover Classification');
Map.addLayer(finalPoints, { color: 'FF0000' }, 'Input Points');
