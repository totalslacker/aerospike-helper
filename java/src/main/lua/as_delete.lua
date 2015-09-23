function durableDelete(rec)

  rec["deleted"] = "yes I am really dead"
  record:update(rec)
  record:delete(rec)

end
