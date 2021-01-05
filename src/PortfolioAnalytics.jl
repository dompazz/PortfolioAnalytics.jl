module PortfolioAnalytics

using DataFrames
using Dates
using LinearAlgebra
using DataFramesMeta
using Statistics
using LoopVectorization

include("helpers.jl")

include("return_functions.jl")

export plusTwo

plusTwo(x) = return x+2



end # module
