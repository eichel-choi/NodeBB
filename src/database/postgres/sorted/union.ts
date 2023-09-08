// I actively made use of ChatGPT to get the outline of the code, please forgive me.

type AggregateType = { $sum: string; $avg: string; };
type ProjectType = { [key: string]: number | string; };
type DataType = {count: number};

type ParamsType = {
    sets: string[];
    start: number;
    stop: number;
    sort: number;
    aggregate?: string;
    withScores?: boolean;
}

type Type1 = { $match: { _key: { $in: string[] } } };
type Type2 = { $group: { _id: { value: string }; totalScore: AggregateType } };
type Type3 = { $sort: { totalScore: number } };
type Type4 = { $skip: number };
type Type5 = { $limit: number };
type Type6 = { $project: ProjectType };
type Type7 = { $group: { _id: { value: string } } };
type Type8 = { $group: { _id: null, count: { $sum: number } } };

type AggregationStage = Type1 | Type2 | Type3 | Type4 | Type5 | Type6 | Type7 | Type8;

module.exports = function (module: {
    getSortedSetRevUnion: (params: ParamsType) => Promise<DataType[]>;
    sortedSetUnionCard(keys: string[]): Promise<number>;
    getSortedSetUnion(params: ParamsType): Promise<DataType[]>;
    client: {
        collection(a: string): {
            aggregate(b: AggregationStage[]): {
                toArray: () => Promise<DataType[]>
            };
        };
    };
  }) {
    module.sortedSetUnionCard = async function (keys: string[]): Promise<number> {
        if (!Array.isArray(keys) || !keys.length) {
            return 0;
        }

        const data = await module.client.collection('objects').aggregate([
            { $match: { _key: { $in: keys } } },
            { $group: { _id: { value: '$value' } } },
            { $group: { _id: null, count: { $sum: 1 } } },
        ]).toArray();
        return Array.isArray(data) && data.length ? data[0].count : 0;
    };

    async function getSortedSetUnion(params: {
        sets: string[];
        start: number;
        stop: number;
        sort: number;
        aggregate?: string;
        withScores?: boolean;
        interval?: number;
      }): Promise<DataType[]> {
        if (!Array.isArray(params.sets) || !params.sets.length) {
            return [];
        }
        let limit = params.stop - params.start + 1;
        if (limit <= 0) {
            limit = 0;
        }

        const aggregate: AggregateType = {
            $sum: '',
            $avg: '',
        };
        if (params.aggregate) {
            aggregate[`$${params.aggregate.toLowerCase()}`] = '$score';
        } else {
            aggregate.$sum = '$score';
        }

        const pipeline: AggregationStage[] = [
            { $match: { _key: { $in: params.sets } } },
            { $group: { _id: { value: '$value' }, totalScore: aggregate } },
            { $sort: { totalScore: params.sort } },
        ];


        if (params.start) {
            pipeline.push({ $skip: params.start });
        }

        if (limit > 0) {
            pipeline.push({ $limit: limit });
        }

        type ProjectType = {
            [key: string]: number | string;
        };

        const project: ProjectType = {
            _id: 0,
            value: '$_id.value',
            score: '$totalScore',
        };
        if (params.withScores) {
            project.score = '$totalScore';
        }
        pipeline.push({ $project: project });

        let data: DataType[] = await module.client.collection('objects').aggregate(pipeline).toArray();
        if (!params.withScores) {
            data = data.map((value: DataType) => value);
        }
        return data;
    }

    module.getSortedSetUnion = async function (params: ParamsType): Promise<DataType[]> {
        params.sort = 1;
        return await getSortedSetUnion(params);
    };

    module.getSortedSetRevUnion = async function (params: ParamsType): Promise<DataType[]> {
        params.sort = -1;
        return await getSortedSetUnion(params);
    };
};
