
function aggDiscreteReturns(returns)
        n = size(returns,1)
        p = 1.0
        @inbounds @avx for i ∈ 1:n
            p = p * (returns[i] + 1.0)
        end
        return(p-1.0)
end

function geo_mean(returns::DataFrame;
                  dateColumn="date")

        vars = names(returns)
        nVars = length(vars)
        vars = Symbol.(vars[vars.!=dateColumn])
        if nVars == length(vars)
          throw(ArgumentError(string("dateColumn: ", dateColumn, " not in DataFrame: ",vars)))
        end
        nVars = nVars-1
        dc = Symbol(dateColumn)

        n = size(returns,1)

        out = DataFrame(:__temp => 1.0)
        for i in 1:nVars
                out[!,vars[i]] .= (1.0+aggDiscreteReturns(returns[!,vars[i]]))^(1/n) - 1.0
        end
        return(out[!,vars])

end

function standard_deviation(returns::DataFrame; scale::Int=1,
                            dateColumn="date",
                            annualized::Bool = false,
                            )

        vars = names(returns)
        nVars = length(vars)
        vars = Symbol.(vars[vars.!=dateColumn])
        if nVars == length(vars)
            throw(ArgumentError(string("dateColumn: ", dateColumn, " not in DataFrame: ",vars)))
        end
        nVars = nVars-1

        out = DataFrame(:_stat_=>"Standard Deviation")


        if annualized
        #   TODO: Test if Threads.@treads is faster for a large number of variables...
                s = convert(Float64,scale)
                for i in 1:nVars
                        out[!,vars[i]] .= std(returns[!,vars[i]]) * sqrt(s)
                end
        else
                for i in 1:nVars
                    out[!,vars[i]] .= std(returns[!,vars[i]])
                end
        end

        return(out)
end
