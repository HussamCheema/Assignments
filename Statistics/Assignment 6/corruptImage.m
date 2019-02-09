function X = corruptImage(fileName,blockSize,displayImage)
%% function will corrupt the image and add missing values as nan
%%% it will also convert an rgb image to grayscale
%%% changing the block size will change the size of the corrupt block
%%% default value for blocksize is 5.  If displayImage is true (default)
%%% then the corrupted image will also be displayed.

if nargin <2
    blockSize = 5;
end

if nargin < 3
    displayImage = true;
end

X = imread(fileName);
X = rgb2gray(X);
X = double(X);
[row col] = size(X);
rowper = randperm(row);

%% add around 20x20 corrupt blocks of size blockSize

for i=1:20
    rowInd = rowper(i);
    colper = randperm(col);
    for j=1:20
        colInd = colper(j);
        lowerR = rowInd;
        upperR = min(rowInd+blockSize,row);
        lowerC = colInd;
        upperC = min(colInd+blockSize,col);
        
        X(lowerR:upperR,lowerC:upperC) = nan;        
        
    end        
end

%% display if flag is set by replacing nan by 0
if displayImage
    X1 = X;
    X1(isnan(X1)) = 0;
    figure, imagesc(X1),colormap gray;
end

