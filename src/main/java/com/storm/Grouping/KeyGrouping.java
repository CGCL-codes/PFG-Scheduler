package com.storm.Grouping;

import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.grouping.CustomStreamGrouping;
import org.apache.storm.task.WorkerTopologyContext;
import org.apache.storm.utils.Utils;

import java.util.Arrays;
import java.util.List;

public class KeyGrouping implements CustomStreamGrouping{
    int _index;
    List<Integer> _targets;
    public static int objectToIndex(Object val, int numPartitions) {
        return val == null ? 0 : Utils.toPositive(val.hashCode()) % numPartitions;
    }

    public KeyGrouping(int index) {
        this._index = index;
    }

    public void prepare(WorkerTopologyContext context, GlobalStreamId stream, List<Integer> targetTasks) {
        this._targets = targetTasks;
    }

    public List<Integer> chooseTasks(int fromTask, List<Object> values) {
        System.out.println(values.get(1)+"----");
        System.exit(0);
        int i = objectToIndex(values.get(this._index), this._targets.size());
        return Arrays.asList((Integer)this._targets.get(i));
    }
}
