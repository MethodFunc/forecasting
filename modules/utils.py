import datetime


def datetime_return(args):
    if args.current:
        start_ = datetime.datetime.now()
        end_ = start_ - datetime.timedelta(days=args.range_)
    else:
        if (args.year is None) or (args.month is None) or (args.days is None):
            raise f'None values is exists. check your parameters'
        start_ = datetime.datetime(args.year, args.month, args.days)
        end_ = start_ - datetime.timedelta(days=args.range_)

    start_ = start_.strftime("%Y-%m-%d")
    end_ = end_.strftime("%Y-%m-%d")

    return start_, end_
