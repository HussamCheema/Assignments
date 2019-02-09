function output = trainRegression(X, Y, Lambda)

    MatrixA = (X'*X);
    I = eye(257,257);
    newI = Lambda .* I;
    B = inv(MatrixA + newI) * (X'*Y);
    
    output = B;

end

