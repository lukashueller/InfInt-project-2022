db.createUser(
    {
        user: "db_user",
        pwd: "db_user",
        roles:
            [
                {
                    role: "readWrite",
                    db: "rb_announcements"
                }
            ]
    }
);