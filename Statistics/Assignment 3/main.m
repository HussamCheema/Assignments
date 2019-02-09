
% Implemented by Hussam-Ul-Hussain
% Roll # 18L-1827

clc; clear; close all;

dat = load('trainingData.txt','-ascii');
testdat = load('testingData.txt','-ascii');

trainLabels = dat(:,end); %all labels are in trainLabels

trainX = dat(:,1:end-1); %all the features are now in trainX

[meanVecClass0,meanVecClass1,covMatrixClass0,covMatrixClass1,prior0,prior1] = learnGauss(trainX, trainLabels);

% disp("Mean Vector class0");
% disp(meanVecClass0);
% disp("Mean Vector class1");
% disp(meanVecClass1);
% disp("covMatrixClass0");
% disp(covMatrixClass0);
% disp("covMatrixClass1");
% disp(covMatrixClass1);
% disp("prior0");
% disp(prior0);
% disp("prior1");
% disp(prior1);

[predictedLabels,MAPClass0,MAPClass1] = testMAP(testdat,meanVecClass0,meanVecClass1,covMatrixClass0,covMatrixClass1,prior0,prior1);

disp("For Complete Covarience Matrix");
disp("MAPClass0 Values");
disp(MAPClass0);
disp("MAPClass1 Values");
disp(MAPClass1);
disp("Predicted Labels");
disp(predictedLabels);

disp("----------------------------------------------");

[meanVecClass0,meanVecClass1,covMatrixClass0,covMatrixClass1,prior0,prior1] = learnGaussDiagonalCov(trainX, trainLabels);

% disp("Mean Vector class0");
% disp(meanVecClass0);
% disp("Mean Vector class1");
% disp(meanVecClass1);
% disp("covMatrixClass0");
% disp(covMatrixClass0);
% disp("covMatrixClass1");
% disp(covMatrixClass1);
% disp("prior0");
% disp(prior0);
% disp("prior1");
% disp(prior1);

[predictedLabels,MAPClass0,MAPClass1] = testMAP(testdat,meanVecClass0,meanVecClass1,covMatrixClass0,covMatrixClass1,prior0,prior1);

disp("For Diagonal Covarience Matrix");
disp("MAPClass0 Values");
disp(MAPClass0);
disp("MAPClass1 Values");
disp(MAPClass1);
disp("Predicted Labels");
disp(predictedLabels);

