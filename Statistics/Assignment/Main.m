clc; clear; close all;

dat = load('trainingData.txt','-ascii');
 
trainLabels = dat(:,end); %all labels are in trainLabels

trainX = dat(:,1:end-1); %all the features are now in trainX

%training the model
[probVecClass0,probVecClass1,prior0,prior1] = learnProb(trainX,trainLabels);

disp("probVecClass0");
disp(probVecClass0)
disp("probVecClass1");
disp(probVecClass1);
disp("prior0");
disp(prior0);
disp("prior1");
disp(prior1);

%loading testing dataset
testdat = load('testingData.txt','-ascii');

%testing using MAP
[predictedLabels,MAPClass0,MAPClass1] = testMAP(testdat, probVecClass0, probVecClass1, prior0, prior1);

disp("MAP VALUES");
disp("MAPClass1");
disp(MAPClass1');
disp("MAPClass0");
disp(MAPClass0');
disp("Predicted Values");
disp(predictedLabels);

%testing using ML
[predictedLabels,MLClass0,MLClass1] = testML(testdat, probVecClass0, probVecClass1, prior0, prior1);

disp("ML VALUES");
disp("MLClass1");
disp(MLClass1');
disp("MLClass0");
disp(MLClass0');
disp("Predicted Values");
disp(predictedLabels);